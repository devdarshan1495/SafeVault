"""Encryption layer using AES-256-GCM."""

import os
from typing import Tuple

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend


AES_KEY_SIZE = 32  # 256 bits
IV_SIZE = 12        # 96 bits for GCM
TAG_SIZE = 16       # 128 bits


class Encryptor:
    def __init__(self, key: bytes = None, enable: bool = True):
        if key is None:
            key = os.urandom(AES_KEY_SIZE)
        self.key = key
        self.enable = enable

    @classmethod
    def from_password(cls, password: str, salt: bytes = None):
        if salt is None:
            salt = os.urandom(16)
        kdf = hashes.Hash(hashes.SHA256(), backend=default_backend())
        kdf.update(password.encode())
        kdf.update(salt)
        key = kdf.finalize()[:AES_KEY_SIZE]
        return cls(key=key)

    def encrypt(self, plaintext: bytes) -> Tuple[bytes, bytes]:
        """Returns (iv + ciphertext + tag, iv)."""
        if not self.enable:
            return plaintext, b""
        iv = os.urandom(IV_SIZE)
        cipher = Cipher(algorithms.AES(self.key), modes.GCM(iv),
                        backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        return iv + ciphertext + encryptor.tag, iv

    def decrypt(self, data: bytes, iv: bytes = None) -> bytes:
        if not self.enable:
            return data
        if iv is None:
            iv = data[:IV_SIZE]
            ciphertext = data[IV_SIZE:-TAG_SIZE]
            tag = data[-TAG_SIZE:]
        else:
            ciphertext = data
            tag = data[-TAG_SIZE:]
            ciphertext = data[:-TAG_SIZE]

        cipher = Cipher(algorithms.AES(self.key), modes.GCM(iv, tag),
                        backend=default_backend())
        decryptor = cipher.decryptor()
        return decryptor.update(ciphertext) + decryptor.finalize()
