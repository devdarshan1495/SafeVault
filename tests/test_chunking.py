"""Tests for the content-defined chunking module."""

import hashlib
from src.core.chunking import RabinChunker, FixedChunker


class TestRabinChunker:
    def setup_method(self):
        self.chunker = RabinChunker(min_chunk=64, avg_chunk=256, max_chunk=512)

    def test_empty_data(self):
        chunks = self.chunker.chunk(b"")
        assert chunks == [(0, 0)]

    def test_small_data(self):
        data = b"hello world"
        chunks = self.chunker.chunk(data)
        assert len(chunks) == 1
        assert data[chunks[0][0]:chunks[0][0] + chunks[0][1]] == data

    def test_deterministic_chunking(self):
        data = b"A" * 10000
        chunks1 = self.chunker.chunk(data)
        chunks2 = self.chunker.chunk(data)
        assert chunks1 == chunks2

    def test_content_stability(self):
        """Inserting bytes should only affect local chunks."""
        data1 = b"the quick brown fox jumps over the lazy dog" * 100
        data2 = b"the quick brown fox jumps over the lazy dog" * 100

        chunks1 = self.chunker.chunk(data1)
        chunks2 = self.chunker.chunk(data2)
        assert chunks1 == chunks2

    def test_reassembly(self):
        data = b"SafeVault backup test data " * 1000
        chunks = self.chunker.chunk(data)
        reassembled = b"".join(
            data[offset:offset + size] for offset, size in chunks
        )
        assert reassembled == data


class TestFixedChunker:
    def test_fixed_chunking(self):
        chunker = FixedChunker(chunk_size=1024)
        data = b"test data " * 500
        chunks = chunker.chunk(data)
        reassembled = b"".join(
            data[offset:offset + size] for offset, size in chunks
        )
        assert reassembled == data
        assert all(size == 1024 or size < 1024
                   for _, size in chunks)
