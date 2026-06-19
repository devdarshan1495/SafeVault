"""Backup engine — orchestrates incremental/full backup jobs."""

import hashlib
import os
import time
from typing import Dict, List, Optional, Tuple

from src.core.chunking import RabinChunker
from src.core.dedup import InMemoryDedupStore
from src.core.compression import Compressor
from src.core.encryption import Encryptor
from src.metadata.store import (
    MetadataStore, BackupType, BackupJob, BackupVersion,
    JobStatus, VersionStatus, FileMetadata, ChunkRecord,
)
from src.messaging.pubsub import PubSubBus, Event, EventTopic
from src.storage.cluster import StorageCluster


class BackupEngine:
    """Coordinates the full backup pipeline for a single job."""

    def __init__(
        self,
        metadata: MetadataStore,
        dedup: InMemoryDedupStore,
        chunker: RabinChunker,
        compressor: Compressor,
        encryptor: Encryptor,
        storage_cluster: StorageCluster,
        pubsub: PubSubBus,
    ):
        self.metadata = metadata
        self.dedup = dedup
        self.chunker = chunker
        self.compressor = compressor
        self.encryptor = encryptor
        self.storage = storage_cluster
        self.pubsub = pubsub

    async def run_backup(self, policy_id: str,
                         source_files: Dict[str, bytes]) -> Optional[str]:
        """Execute a full or incremental backup for a policy.

        Args:
            policy_id: The backup policy to run.
            source_files: Dict of file_path → file_content bytes.

        Returns:
            job_id if successful, None otherwise.
        """
        policy = self.metadata.get_policy(policy_id)
        if not policy:
            raise ValueError(f"Policy {policy_id} not found")

        last_version = self.metadata.get_latest_version(policy_id)
        backup_type = BackupType.FULL
        parent_version_id = None

        if last_version:
            backup_type = BackupType.INCREMENTAL
            parent_version_id = last_version.id

        # Create job
        job = self.metadata.create_job(policy_id, policy.user_id, backup_type)
        self.metadata.update_job_status(job.id, JobStatus.RUNNING,
                                        started_at=time.time())

        await self.pubsub.publish(Event(
            topic=EventTopic.BACKUP_JOB_STARTED,
            event_type=backup_type.value,
            payload={"job_id": job.id, "policy_id": policy_id,
                     "type": backup_type.value},
            producer="backup-engine",
        ))

        try:
            # Determine version number
            version_number = 1
            if last_version:
                version_number = last_version.version_number + 1

            version = self.metadata.create_version(
                job_id=job.id,
                policy_id=policy_id,
                user_id=policy.user_id,
                version_number=version_number,
                parent_version_id=parent_version_id,
                backup_type=backup_type,
                retention_days=policy.retention_days,
            )

            total_bytes = 0
            new_chunks = 0
            dedup_savings = 0
            files_processed = 0

            for file_path, content in source_files.items():
                fm = self._process_file(
                    file_path, content, policy, version, job)
                if fm:
                    files_processed += 1
                    total_bytes += fm.file_size

            # Update job
            total_unique = self.dedup.total_unique_chunks()
            total_stored = self.dedup.total_storage_bytes()
            savings = max(0, total_bytes - total_stored)

            self.metadata.update_job_status(
                job.id, JobStatus.COMPLETED,
                completed_at=time.time(),
                total_bytes=total_bytes,
                new_chunks_count=total_unique,
                dedup_savings_bytes=savings,
            )

            await self.pubsub.publish(Event(
                topic=EventTopic.BACKUP_JOB_COMPLETED,
                event_type=backup_type.value,
                payload={"job_id": job.id, "version_id": version.id,
                         "policy_id": policy_id, "files": files_processed,
                         "bytes": total_bytes, "dedup_savings": savings,
                         "unique_chunks": total_unique},
                producer="backup-engine",
            ))

            return job.id

        except Exception as e:
            self.metadata.update_job_status(
                job.id, JobStatus.FAILED,
                completed_at=time.time(),
                error_message=str(e),
            )
            await self.pubsub.publish(Event(
                topic=EventTopic.BACKUP_JOB_FAILED,
                event_type="error",
                payload={"job_id": job.id, "policy_id": policy_id,
                         "error": str(e)},
                producer="backup-engine",
            ))
            return None

    def _process_file(
        self,
        file_path: str,
        content: bytes,
        policy,
        version: BackupVersion,
        job: BackupJob,
    ) -> Optional[FileMetadata]:
        """Process a single file through the chunk → dedup → compress → encrypt → store pipeline."""
        if not content:
            return None

        file_hash = hashlib.sha256(content).hexdigest()
        fm = self.metadata.register_file(
            policy.user_id, file_path, len(content),
            time.time(), file_hash,
        )

        chunks = self.chunker.chunk(content)

        for idx, (offset, size) in enumerate(chunks):
            chunk_data = content[offset:offset + size]

            # Dedup
            chunk_hash = self.dedup.store(chunk_data)
            is_new = self.dedup.ref_count(chunk_hash) == 1

            # Compress (only for new chunks)
            compressed = self.compressor.compress(chunk_data)

            # Encrypt
            encrypted, iv = self.encryptor.encrypt(compressed)

            # Store to cluster
            locations = self.storage.quorum_write(chunk_hash, encrypted)

            # Register metadata
            record = self.metadata.register_chunk(
                chunk_hash, len(chunk_data), len(compressed),
            )

            for node_id, path in locations:
                self.metadata.add_chunk_location(
                    record.id, node_id, path,
                    primary=(node_id == locations[0][0]),
                )

            self.metadata.link_file_chunk(
                fm.id, record.id, idx, offset, size,
            )

        return fm

    def simulate_files(self, base_path: str = "/data") -> Dict[str, bytes]:
        """Generate simulated files for testing."""
        return {
            f"{base_path}/document.txt": b"Hello World! " * 1000,
            f"{base_path}/database.dump": os.urandom(100_000),
            f"{base_path}/config.json": b'{"key": "value", "nested": [1,2,3]}',
            f"{base_path}/image.png": os.urandom(200_000),
            f"{base_path}/log.txt": b"2024-01-01 INFO: started\n" * 5000,
        }

    def simulate_modified_files(self) -> Dict[str, bytes]:
        """Generate modified files (for incremental backup testing)."""
        return {
            "/data/document.txt": b"Hello World! " * 1001,  # Slightly modified
            "/data/database.dump": os.urandom(100_000),     # Completely changed
            "/data/new_file.csv": b"a,b,c\n1,2,3\n",        # New file
        }
