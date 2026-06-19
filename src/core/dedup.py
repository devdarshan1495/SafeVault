"""Deduplication engine with SHA-256 hash index and Bloom filter."""

import hashlib
from typing import Dict, Optional, Set


class InMemoryDedupStore:
    """In-memory dedup store for simulation. Thread-safe for single process."""

    def __init__(self, enable_bloom: bool = True):
        self._chunks: Dict[str, bytes] = {}  # hash → chunk_data
        self._ref_counts: Dict[str, int] = {}  # hash → reference count
        self._bloom: Set[str] = set()  # Simplified bloom filter (set)
        self.enable_bloom = enable_bloom

    def _hash(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def might_exist(self, chunk_hash: str) -> bool:
        if self.enable_bloom:
            return chunk_hash in self._bloom
        return chunk_hash in self._chunks

    def store(self, chunk_data: bytes) -> str:
        chunk_hash = self._hash(chunk_data)

        if chunk_hash in self._chunks:
            self._ref_counts[chunk_hash] += 1
            return chunk_hash

        self._chunks[chunk_hash] = chunk_data
        self._ref_counts[chunk_hash] = 1
        if self.enable_bloom:
            self._bloom.add(chunk_hash)
        return chunk_hash

    def lookup(self, chunk_hash: str) -> Optional[bytes]:
        return self._chunks.get(chunk_hash)

    def release(self, chunk_hash: str) -> None:
        if chunk_hash in self._ref_counts:
            self._ref_counts[chunk_hash] -= 1
            if self._ref_counts[chunk_hash] <= 0:
                del self._chunks[chunk_hash]
                del self._ref_counts[chunk_hash]
                if self.enable_bloom:
                    self._bloom.discard(chunk_hash)

    def ref_count(self, chunk_hash: str) -> int:
        return self._ref_counts.get(chunk_hash, 0)

    def total_unique_chunks(self) -> int:
        return len(self._chunks)

    def total_storage_bytes(self) -> int:
        return sum(len(c) for c in self._chunks.values())
