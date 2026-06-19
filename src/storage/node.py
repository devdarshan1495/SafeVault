"""Simulated distributed storage node."""

import os
import uuid
import time
from dataclasses import dataclass, field
from typing import Dict, Optional
from enum import Enum


class NodeStatus(Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"


@dataclass
class StoredChunk:
    hash: str
    data: bytes
    size: int
    stored_at: float = field(default_factory=time.time)


@dataclass
class StorageNode:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    region: str = "us-east-1"
    zone: str = "us-east-1a"
    capacity_bytes: int = 1_000_000_000_000  # 1 TB default
    status: NodeStatus = NodeStatus.ONLINE

    def __post_init__(self):
        if not self.name:
            self.name = f"node-{self.id[:8]}"
        self._store: Dict[str, StoredChunk] = {}
        self._used_bytes = 0

    @property
    def used_bytes(self) -> int:
        return self._used_bytes

    @property
    def available_bytes(self) -> int:
        return self.capacity_bytes - self._used_bytes

    def store(self, chunk_hash: str, data: bytes) -> bool:
        if self.status == NodeStatus.OFFLINE:
            raise RuntimeError(f"Node {self.name} is offline")
        if self.status == NodeStatus.MAINTENANCE:
            raise RuntimeError(f"Node {self.name} is under maintenance")
        if len(data) > self.available_bytes:
            raise RuntimeError(f"Node {self.name} out of capacity")
        if chunk_hash in self._store:
            return False
        # Store with the computed SHA-256 for integrity verification
        stored_hash = self._compute_hash(data)
        self._store[chunk_hash] = StoredChunk(hash=stored_hash, data=data,
                                               size=len(data))
        self._used_bytes += len(data)
        return True

    def retrieve(self, chunk_hash: str) -> Optional[bytes]:
        if self.status == NodeStatus.OFFLINE:
            raise RuntimeError(f"Node {self.name} is offline")
        if chunk_hash not in self._store:
            return None
        chunk = self._store[chunk_hash]
        # Simulate silent data corruption check
        data_hash = self._compute_hash(chunk.data)
        if data_hash != chunk.hash:
            raise RuntimeError(f"Checksum mismatch for chunk {chunk_hash} "
                               f"on node {self.name}")
        return chunk.data

    def delete(self, chunk_hash: str) -> bool:
        if chunk_hash not in self._store:
            return False
        self._used_bytes -= self._store[chunk_hash].size
        del self._store[chunk_hash]
        return True

    def has_chunk(self, chunk_hash: str) -> bool:
        return chunk_hash in self._store

    def chunk_count(self) -> int:
        return len(self._store)

    def set_status(self, status: NodeStatus) -> None:
        self.status = status

    @staticmethod
    def _compute_hash(data: bytes) -> str:
        import hashlib
        return hashlib.sha256(data).hexdigest()
