"""In-memory metadata store simulating PostgreSQL + Redis.

In production this would use PostgreSQL for relational data
and Redis for caching/queues as described in database_schema.md.
"""

import uuid
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional, Tuple


class JobStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class VersionStatus(Enum):
    ACTIVE = "active"
    EXPIRED = "expired"
    DELETED = "deleted"


class BackupType(Enum):
    FULL = "full"
    INCREMENTAL = "incremental"


@dataclass
class BackupPolicy:
    id: str
    user_id: str
    name: str
    schedule_cron: str
    retention_days: int
    backup_type: BackupType
    source_paths: List[str]
    enabled: bool = True
    created_at: float = field(default_factory=time.time)


@dataclass
class BackupJob:
    id: str
    policy_id: str
    user_id: str
    backup_type: BackupType
    status: JobStatus = JobStatus.PENDING
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    total_bytes: int = 0
    new_chunks_count: int = 0
    dedup_savings_bytes: int = 0
    error_message: Optional[str] = None
    checkpoint_path: Optional[str] = None
    created_at: float = field(default_factory=time.time)


@dataclass
class BackupVersion:
    id: str
    job_id: str
    policy_id: str
    user_id: str
    version_number: int
    parent_version_id: Optional[str]
    backup_type: BackupType
    snapshot_time: float
    status: VersionStatus = VersionStatus.ACTIVE
    expires_at: Optional[float] = None
    created_at: float = field(default_factory=time.time)


@dataclass
class FileMetadata:
    id: str
    user_id: str
    file_path: str
    file_size: int
    file_mod_time: float
    file_hash: str
    created_at: float = field(default_factory=time.time)


@dataclass
class ChunkRecord:
    id: str
    hash: str
    size_bytes: int
    compressed_size_bytes: int
    reference_count: int = 1
    created_at: float = field(default_factory=time.time)


@dataclass
class FileChunk:
    file_id: str
    chunk_id: str
    chunk_index: int
    offset: int
    size_bytes: int


@dataclass
class ChunkLocation:
    chunk_id: str
    storage_node_id: str
    storage_path: str
    is_primary: bool


@dataclass
class RestoreJob:
    id: str
    user_id: str
    version_id: str
    restore_path: str
    status: JobStatus = JobStatus.PENDING
    files_restored: int = 0
    total_bytes: int = 0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error_message: Optional[str] = None
    created_at: float = field(default_factory=time.time)


class MetadataStore:
    """In-memory metadata store for simulation."""

    def __init__(self):
        self.policies: Dict[str, BackupPolicy] = {}
        self.jobs: Dict[str, BackupJob] = {}
        self.versions: Dict[str, BackupVersion] = {}
        self.files: Dict[str, FileMetadata] = {}
        self.chunks: Dict[str, ChunkRecord] = {}
        self.file_chunks: Dict[str, List[FileChunk]] = {}  # file_id → chunks
        self.chunk_locations: Dict[str, List[ChunkLocation]] = {}
        self.restore_jobs: Dict[str, RestoreJob] = {}
        self._file_path_index: Dict[str, str] = {}  # path → file_id

    # --- Policies ---
    def create_policy(self, user_id: str, name: str, schedule_cron: str,
                      retention_days: int, backup_type: BackupType,
                      source_paths: List[str]) -> BackupPolicy:
        policy = BackupPolicy(
            id=str(uuid.uuid4()),
            user_id=user_id,
            name=name,
            schedule_cron=schedule_cron,
            retention_days=retention_days,
            backup_type=backup_type,
            source_paths=source_paths,
        )
        self.policies[policy.id] = policy
        return policy

    def get_policy(self, policy_id: str) -> Optional[BackupPolicy]:
        return self.policies.get(policy_id)

    # --- Files ---
    def register_file(self, user_id: str, file_path: str, file_size: int,
                      file_mod_time: float, file_hash: str) -> FileMetadata:
        existing_id = self._file_path_index.get(file_path)
        if existing_id and existing_id in self.files:
            existing = self.files[existing_id]
            existing.file_size = file_size
            existing.file_mod_time = file_mod_time
            existing.file_hash = file_hash
            return existing
        fm = FileMetadata(
            id=str(uuid.uuid4()),
            user_id=user_id,
            file_path=file_path,
            file_size=file_size,
            file_mod_time=file_mod_time,
            file_hash=file_hash,
        )
        self.files[fm.id] = fm
        self._file_path_index[file_path] = fm.id
        return fm

    def get_file_by_path(self, path: str) -> Optional[FileMetadata]:
        fid = self._file_path_index.get(path)
        return self.files.get(fid) if fid else None

    # --- Jobs ---
    def create_job(self, policy_id: str, user_id: str,
                   backup_type: BackupType) -> BackupJob:
        job = BackupJob(
            id=str(uuid.uuid4()),
            policy_id=policy_id,
            user_id=user_id,
            backup_type=backup_type,
        )
        self.jobs[job.id] = job
        return job

    def update_job_status(self, job_id: str, status: JobStatus,
                          **kwargs) -> None:
        job = self.jobs.get(job_id)
        if job:
            job.status = status
            for k, v in kwargs.items():
                setattr(job, k, v)

    # --- Versions ---
    def create_version(self, job_id: str, policy_id: str, user_id: str,
                       version_number: int, parent_version_id: Optional[str],
                       backup_type: BackupType,
                       retention_days: int) -> BackupVersion:
        expires_at = (time.time() + retention_days * 86400
                      if retention_days else None)
        ver = BackupVersion(
            id=str(uuid.uuid4()),
            job_id=job_id,
            policy_id=policy_id,
            user_id=user_id,
            version_number=version_number,
            parent_version_id=parent_version_id,
            backup_type=backup_type,
            snapshot_time=time.time(),
            expires_at=expires_at,
        )
        self.versions[ver.id] = ver
        return ver

    def get_latest_version(self, policy_id: str) -> Optional[BackupVersion]:
        versions = [v for v in self.versions.values()
                    if v.policy_id == policy_id and v.status == VersionStatus.ACTIVE]
        if not versions:
            return None
        return max(versions, key=lambda v: v.version_number)

    # --- Chunks ---
    def register_chunk(self, chunk_hash: str, size_bytes: int,
                       compressed_size_bytes: int) -> ChunkRecord:
        if chunk_hash in self.chunks:
            self.chunks[chunk_hash].reference_count += 1
            return self.chunks[chunk_hash]
        record = ChunkRecord(
            id=str(uuid.uuid4()),
            hash=chunk_hash,
            size_bytes=size_bytes,
            compressed_size_bytes=compressed_size_bytes,
        )
        self.chunks[chunk_hash] = record
        return record

    def add_chunk_location(self, chunk_id: str, node_id: str,
                           path: str, primary: bool = True) -> None:
        loc = ChunkLocation(
            chunk_id=chunk_id,
            storage_node_id=node_id,
            storage_path=path,
            is_primary=primary,
        )
        self.chunk_locations.setdefault(chunk_id, []).append(loc)

    def link_file_chunk(self, file_id: str, chunk_id: str, chunk_index: int,
                        offset: int, size: int) -> None:
        fc = FileChunk(
            file_id=file_id,
            chunk_id=chunk_id,
            chunk_index=chunk_index,
            offset=offset,
            size_bytes=size,
        )
        self.file_chunks.setdefault(file_id, []).append(fc)

    def get_file_chunks(self, file_id: str) -> List[FileChunk]:
        return sorted(self.file_chunks.get(file_id, []),
                      key=lambda fc: fc.chunk_index)

    # --- Restore ---
    def create_restore_job(self, user_id: str, version_id: str,
                           restore_path: str) -> RestoreJob:
        rj = RestoreJob(
            id=str(uuid.uuid4()),
            user_id=user_id,
            version_id=version_id,
            restore_path=restore_path,
        )
        self.restore_jobs[rj.id] = rj
        return rj

    def update_restore_job(self, job_id: str, **kwargs) -> None:
        rj = self.restore_jobs.get(job_id)
        if rj:
            for k, v in kwargs.items():
                setattr(rj, k, v)
