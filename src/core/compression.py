"""Compression layer using zstandard (zstd)."""

import zstandard as zstd
from typing import Union


class Compressor:
    def __init__(self, level: int = 3, enable: bool = True):
        self._cctx = zstd.ZstdCompressor(level=level)
        self._dctx = zstd.ZstdDecompressor()
        self.enable = enable

    def compress(self, data: Union[bytes, bytearray]) -> bytes:
        if not self.enable:
            return bytes(data)
        return self._cctx.compress(data)

    def decompress(self, data: Union[bytes, bytearray]) -> bytes:
        if not self.enable:
            return bytes(data)
        return self._dctx.decompress(data)
