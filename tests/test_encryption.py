"""Tests for the encryption module."""

import os
import pytest
from src.core.encryption import Encryptor


class TestEncryptor:
    def test_encrypt_decrypt_roundtrip(self):
        e = Encryptor()
        data = b"secret backup data"
        encrypted, iv = e.encrypt(data)
        assert encrypted != data
        assert len(encrypted) > len(data)
        decrypted = e.decrypt(encrypted)
        assert decrypted == data

    def test_disabled_encryption(self):
        e = Encryptor(enable=False)
        data = b"test"
        encrypted, iv = e.encrypt(data)
        assert encrypted == data

    def test_different_keys_produce_different_ciphertext(self):
        data = b"same data"
        e1 = Encryptor()
        e2 = Encryptor()
        ct1, _ = e1.encrypt(data)
        ct2, _ = e2.encrypt(data)
        assert ct1 != ct2

    def test_tampered_ciphertext_fails(self):
        e = Encryptor()
        data = b"tamper test"
        encrypted, iv = e.encrypt(data)
        tampered = bytearray(encrypted)
        tampered[10] ^= 0xFF
        with pytest.raises(Exception):
            e.decrypt(bytes(tampered))

    def test_empty_data(self):
        e = Encryptor()
        encrypted, iv = e.encrypt(b"")
        assert e.decrypt(encrypted) == b""

    def test_large_data(self):
        e = Encryptor()
        data = os.urandom(10000)
        encrypted, iv = e.encrypt(data)
        assert e.decrypt(encrypted) == data

    def test_from_password(self):
        e = Encryptor.from_password("my-secret-password")
        data = b"password protected data"
        encrypted, iv = e.encrypt(data)
        assert e.decrypt(encrypted) == data
