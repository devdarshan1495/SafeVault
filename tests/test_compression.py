"""Tests for the compression module."""

import os
from src.core.compression import Compressor


class TestCompressor:
    def test_compress_decompress_roundtrip(self):
        c = Compressor(level=3)
        data = b"A" * 10000
        compressed = c.compress(data)
        assert len(compressed) < len(data) or len(compressed) == len(data)
        decompressed = c.decompress(compressed)
        assert decompressed == data

    def test_disabled_compression(self):
        c = Compressor(enable=False)
        data = b"test data"
        assert c.compress(data) == data
        assert c.decompress(data) == data

    def test_empty_data(self):
        c = Compressor()
        assert c.decompress(c.compress(b"")) == b""

    def test_random_binary_data(self):
        c = Compressor(level=1)
        data = os.urandom(5000)
        assert c.decompress(c.compress(data)) == data

    def test_different_levels(self):
        data = b"Hello SafeVault! " * 1000
        c1 = Compressor(level=1)
        c3 = Compressor(level=3)
        c9 = Compressor(level=9)
        # Higher compression should produce smaller output
        assert len(c9.compress(data)) <= len(c3.compress(data)) <= len(c1.compress(data))
