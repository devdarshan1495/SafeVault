"""Content-Defined Chunking using Rabin fingerprint algorithm."""

import struct
from typing import List, Tuple

RABIN_POLY = 0x3DA3358B4DC173
RABIN_WINDOW_SIZE = 48
RABIN_MODULUS = (1 << 31) - 1
RABIN_TARGET_MASK = 0x00001FFF
DEFAULT_MIN_CHUNK = 4096
DEFAULT_AVG_CHUNK = 8192
DEFAULT_MAX_CHUNK = 16384


class RabinChunker:
    """Splits byte streams into variable-size content-defined chunks."""

    def __init__(
        self,
        min_chunk: int = DEFAULT_MIN_CHUNK,
        avg_chunk: int = DEFAULT_AVG_CHUNK,
        max_chunk: int = DEFAULT_MAX_CHUNK,
    ):
        self.min_chunk = min_chunk
        self.avg_chunk = avg_chunk
        self.max_chunk = max_chunk
        self._table = self._build_table()

    @staticmethod
    def _build_table() -> List[int]:
        table = []
        for b in range(256):
            hash_val = b
            for _ in range(RABIN_WINDOW_SIZE):
                for bit in range(8):
                    if hash_val & 0x80000000:
                        hash_val = (hash_val << 1) ^ RABIN_POLY
                    else:
                        hash_val <<= 1
                    hash_val &= 0xFFFFFFFF
            table.append(hash_val)
        return table

    def _rabin_fingerprint(self, data: bytes, start: int) -> int:
        hash_val = 0
        end = min(start + RABIN_WINDOW_SIZE, len(data))
        for b in data[start:end]:
            hash_val = ((hash_val << 1) | b) & 0xFFFFFFFF
        return hash_val

    def chunk(self, data: bytes) -> List[Tuple[int, int]]:
        """Return list of (offset, size) tuples describing chunk boundaries."""
        if not data:
            return [(0, 0)]

        boundaries = []
        last_boundary = 0
        hash_val = 0

        for i in range(len(data)):
            if i < RABIN_WINDOW_SIZE:
                hash_val = ((hash_val << 1) + data[i]) % RABIN_MODULUS
            else:
                out_byte = data[i - RABIN_WINDOW_SIZE]
                in_byte = data[i]
                hash_val = (
                    (hash_val << 1) - (out_byte << RABIN_WINDOW_SIZE) + in_byte
                ) % RABIN_MODULUS

            chunk_len = i - last_boundary + 1

            if chunk_len >= self.min_chunk:
                if (hash_val & RABIN_TARGET_MASK) == 0:
                    boundaries.append(i + 1)
                    last_boundary = i + 1
                    continue

            if chunk_len >= self.max_chunk:
                boundaries.append(i + 1)
                last_boundary = i + 1

        if last_boundary < len(data):
            boundaries.append(len(data))

        result = []
        prev = 0
        for b in boundaries:
            result.append((prev, b - prev))
            prev = b
        return result


class FixedChunker:
    """Simple fixed-size chunker for comparison/testing."""

    def __init__(self, chunk_size: int = 8192):
        self.chunk_size = chunk_size

    def chunk(self, data: bytes) -> List[Tuple[int, int]]:
        result = []
        offset = 0
        while offset < len(data):
            size = min(self.chunk_size, len(data) - offset)
            result.append((offset, size))
            offset += size
        return result
