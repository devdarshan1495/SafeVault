"""Tests for the deduplication engine."""

import os
from src.core.dedup import InMemoryDedupStore


class TestInMemoryDedupStore:
    def setup_method(self):
        self.dedup = InMemoryDedupStore()

    def test_store_and_lookup(self):
        data = b"hello world"
        h = self.dedup.store(data)
        assert self.dedup.lookup(h) == data

    def test_dedup_identical_chunks(self):
        data = b"test data " * 100
        h1 = self.dedup.store(data)
        h2 = self.dedup.store(data)
        assert h1 == h2
        assert self.dedup.ref_count(h1) == 2

    def test_ref_count_decrement(self):
        data = b"refcount test"
        h = self.dedup.store(data)
        assert self.dedup.ref_count(h) == 1
        self.dedup.store(data)
        assert self.dedup.ref_count(h) == 2
        self.dedup.release(h)
        assert self.dedup.ref_count(h) == 1

    def test_release_removes_data(self):
        data = b"unique data"
        h = self.dedup.store(data)
        assert self.dedup.lookup(h) == data
        self.dedup.release(h)
        assert self.dedup.lookup(h) is None

    def test_unique_chunks(self):
        self.dedup.store(b"chunk_a")
        self.dedup.store(b"chunk_b")
        self.dedup.store(b"chunk_a")  # duplicate
        assert self.dedup.total_unique_chunks() == 2

    def test_storage_bytes(self):
        self.dedup.store(b"a" * 100)
        self.dedup.store(b"b" * 200)
        assert self.dedup.total_storage_bytes() == 300
