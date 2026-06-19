"""Restore engine — snapshot-based recovery with version chain traversal."""

import asyncio
import time
from typing import Dict, List, Optional

from src.core.dedup import InMemoryDedupStore
from src.core.compression import Compressor
from src.core.encryption import Encryptor
from src.metadata.store import (
    MetadataStore, BackupType, BackupVersion, FileMetadata,
    FileChunk, ChunkRecord, JobStatus,
)
from src.messaging.pubsub import PubSubBus, Event, EventTopic
from src.storage.cluster import StorageCluster


class RestoreEngine:
    """Handles point-in-time and latest-version file recovery."""

    def __init__(
        self,
        metadata: MetadataStore,
        dedup: InMemoryDedupStore,
        compressor: Compressor,
        encryptor: Encryptor,
        storage_cluster: StorageCluster,
        pubsub: PubSubBus,
    ):
        self.metadata = metadata
        self.dedup = dedup
        self.compressor = compressor
        self.encryptor = encryptor
        self.storage = storage_cluster
        self.pubsub = pubsub

    async def restore_latest(self, user_id: str, policy_id: str,
                             file_path: str,
                             restore_path: str) -> Optional[bytes]:
        """Restore the latest version of a file."""
        version = self.metadata.get_latest_version(policy_id)
        if not version:
            raise ValueError(f"No versions found for policy {policy_id}")
        return await self._restore_file(
            user_id, version, file_path, restore_path)

    async def restore_point_in_time(self, user_id: str, policy_id: str,
                                    file_path: str, target_timestamp: float,
                                    restore_path: str) -> Optional[bytes]:
        """Restore a file as it existed at a specific timestamp."""
        # Find the latest version at or before target_timestamp
        candidates = [
            v for v in self.metadata.versions.values()
            if v.policy_id == policy_id
            and v.status.value == "active"
            and v.snapshot_time <= target_timestamp
        ]
        if not candidates:
            raise ValueError(f"No version at timestamp {target_timestamp}")
        version = max(candidates, key=lambda v: v.snapshot_time)
        return await self._restore_file(
            user_id, version, file_path, restore_path)

    async def _restore_file(self, user_id: str, version: BackupVersion,
                            file_path: str,
                            restore_path: str) -> Optional[bytes]:
        """Walk the version chain and reconstruct a file from chunks."""
        # Create restore job
        job = self.metadata.create_restore_job(user_id, version.id,
                                                restore_path)
        self.metadata.update_restore_job(job.id, status=JobStatus.RUNNING,
                                         started_at=time.time())

        await self.pubsub.publish(Event(
            topic=EventTopic.RESTORE_JOB_STARTED,
            event_type="restore",
            payload={"job_id": job.id, "version_id": version.id,
                     "file_path": file_path},
            producer="restore-engine",
        ))

        try:
            file_meta = self.metadata.get_file_by_path(file_path)
            if not file_meta:
                raise FileNotFoundError(f"File {file_path} not found in backup")

            # Collect all chunks from version chain
            chunk_hashes_ordered = self._collect_chunk_hashes(
                version, file_meta.id)

            # Download, decrypt, decompress chunks in parallel
            reconstructed = bytearray()
            for chunk_hash in chunk_hashes_ordered:
                chunk_data = await self._retrieve_chunk(chunk_hash)
                reconstructed.extend(chunk_data)

            result = bytes(reconstructed)

            # Verify integrity
            import hashlib
            if hashlib.sha256(result).hexdigest() != file_meta.file_hash:
                raise RuntimeError(
                    f"Integrity check failed for {file_path}")

            self.metadata.update_restore_job(
                job.id, status=JobStatus.COMPLETED,
                completed_at=time.time(),
                files_restored=1,
                total_bytes=len(result),
            )

            await self.pubsub.publish(Event(
                topic=EventTopic.RESTORE_JOB_COMPLETED,
                event_type="restore",
                payload={"job_id": job.id, "version_id": version.id,
                         "file_path": file_path, "bytes": len(result)},
                producer="restore-engine",
            ))

            return result

        except Exception as e:
            self.metadata.update_restore_job(
                job.id, status=JobStatus.FAILED,
                completed_at=time.time(),
                error_message=str(e),
            )
            await self.pubsub.publish(Event(
                topic=EventTopic.RESTORE_JOB_FAILED,
                event_type="error",
                payload={"job_id": job.id, "version_id": version.id,
                         "error": str(e)},
                producer="restore-engine",
            ))
            raise

    def _collect_chunk_hashes(self, version: BackupVersion,
                              file_id: str) -> List[str]:
        """Walk the version chain and collect chunk SHA-256 hashes in order."""
        all_chunks: Dict[int, FileChunk] = {}
        current = version

        while current is not None:
            file_chunks = self.metadata.get_file_chunks(file_id)
            for fc in file_chunks:
                if fc.chunk_index not in all_chunks:
                    all_chunks[fc.chunk_index] = fc
                # If this is an incremental version, later chunks override
                elif current.backup_type == BackupType.INCREMENTAL:
                    all_chunks[fc.chunk_index] = fc

            if current.backup_type == BackupType.FULL:
                break
            if current.parent_version_id:
                current = self.metadata.versions.get(
                    current.parent_version_id)
            else:
                break

        # Map chunk UUIDs → SHA-256 hashes
        chunk_id_to_hash = {}
        for chunk_hash, record in self.metadata.chunks.items():
            chunk_id_to_hash[record.id] = chunk_hash

        sorted_indices = sorted(all_chunks.keys())
        result = []
        for idx in sorted_indices:
            fc = all_chunks[idx]
            chunk_hash = chunk_id_to_hash.get(fc.chunk_id)
            if not chunk_hash:
                raise RuntimeError(
                    f"Chunk record {fc.chunk_id} not found for file {file_id}")
            result.append(chunk_hash)
        return result

    async def _retrieve_chunk(self, chunk_hash: str) -> bytes:
        """Retrieve, decrypt, and decompress a single chunk."""
        # Try dedup store first (in-memory simulation)
        chunk_data = self.dedup.lookup(chunk_hash)
        if chunk_data is not None:
            return chunk_data

        # Fall back to storage cluster
        encrypted = self.storage.quorum_read(chunk_hash)
        if encrypted is None:
            raise RuntimeError(f"Chunk {chunk_hash} not found in cluster")

        # Decrypt
        compressed = self.encryptor.decrypt(encrypted)

        # Decompress
        return self.compressor.decompress(compressed)
