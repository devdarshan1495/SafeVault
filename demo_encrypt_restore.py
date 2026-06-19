"""
SafeVault Viva Demo — modify data → backup (encrypted) → restore (decrypted).
Run with:  python3 demo_encrypt_restore.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from src.core.chunking import RabinChunker
from src.core.dedup import InMemoryDedupStore
from src.core.compression import Compressor
from src.core.encryption import Encryptor
from src.metadata.store import MetadataStore, BackupType
from src.messaging.pubsub import PubSubBus
from src.storage.node import StorageNode
from src.storage.cluster import StorageCluster
from src.backup.engine import BackupEngine
from src.restore.engine import RestoreEngine


async def main():
    print("=" * 60)
    print("SafeVault Demo — Backup & Restore with Encryption")
    print("=" * 60)

    # ---- 1. Setup components ----
    pubsub = PubSubBus()
    metadata = MetadataStore()
    dedup = InMemoryDedupStore()
    chunker = RabinChunker()
    compressor = Compressor(level=3)
    encryptor = Encryptor()

    cluster = StorageCluster(replication_factor=3)
    for r, z in [("us-east-1", "a"), ("us-east-1", "b"), ("us-east-1", "c"),
                 ("eu-west-1", "a"), ("eu-west-1", "b"), ("ap-south-1", "a")]:
        cluster.add_node(StorageNode(name=f"node-{r}-{z}", region=r, zone=z))

    backup_engine = BackupEngine(metadata, dedup, chunker, compressor, encryptor, cluster, pubsub)
    restore_engine = RestoreEngine(metadata, dedup, compressor, encryptor, cluster, pubsub)

    # ---- 2. Create a backup policy ----
    policy = metadata.create_policy("viva-user", "Viva Demo", "0 2 * * *", 30, BackupType.FULL, ["/demo"])
    print(f"\n[1] Backup policy created — ID: {policy.id[:8]}...\n")

    # ---- 3. ORIGINAL data → backup ----
    original = b"Hello SafeVault! This is my important document. " * 100
    files_v1 = {"/demo/notes.txt": original}
    print(f"[2] Original file  : {len(original)} bytes  [{original[:50]}...]")

    job1 = await backup_engine.run_backup(policy.id, files_v1)
    j1 = metadata.jobs[job1]
    print(f"[3] Full backup    : job={job1[:8]}... | chunks={j1.new_chunks_count} | "
          f"encrypted & replicated to {cluster.node_count()} nodes\n")

    # ---- 4. MODIFIED data → incremental backup ----
    modified = b"Hello SafeVault! This is my MODIFIED document. " * 100
    files_v2 = {"/demo/notes.txt": modified}
    print(f"[4] Modified file  : {len(modified)} bytes  [{modified[:50]}...]")

    job2 = await backup_engine.run_backup(policy.id, files_v2)
    j2 = metadata.jobs[job2]
    print(f"[5] Incremental bkp: job={job2[:8]}... | type={j2.backup_type.value} | "
          f"new_chunks={j2.new_chunks_count} (rest deduplicated)\n")

    # ---- 5. Restore latest (gets MODIFIED content) ----
    print("-" * 60)
    print("RESTORE — latest version (should return MODIFIED data)")
    print("-" * 60)
    result = await restore_engine.restore_latest(
        "viva-user", policy.id, "/demo/notes.txt",
        restore_path="/restore/notes.txt",
    )
    match = result == modified
    print(f"  Restored: {len(result)} bytes | [{result[:50]}...]")
    print(f"  Integrity: {'✓ MATCHES modified' if match else '✗ MISMATCH'}\n")

    # ---- 6. Show encryption in action ----
    print("=" * 60)
    print("ENCRYPTION — same chunk before vs after AES-256-GCM")
    print("=" * 60)
    sample_hash = list(dedup._chunks.keys())[0]
    raw = dedup._chunks[sample_hash]
    comp = compressor.compress(raw)
    enc, iv = encryptor.encrypt(comp)
    dec = encryptor.decrypt(enc)
    decomp = compressor.decompress(dec)

    print(f"\n  Plaintext  ({len(raw)} bytes):  {raw[:50]}")
    print(f"  Encrypted  ({len(enc)} bytes):  {enc[:50]!r}")
    print(f"  Decrypted  ({len(decomp)} bytes): {decomp[:50]}")
    print(f"  Round-trip: {'✓ PASS' if raw == decomp else '✗ FAIL'}")

    # ---- 7. Show that same plaintext produces different ciphertext ----
    enc2, iv2 = encryptor.encrypt(comp)
    print(f"\n  Same data encrypted twice — ciphertexts differ:")
    print(f"  Attempt 1: {enc[:20]!r}...")
    print(f"  Attempt 2: {enc2[:20]!r}...")
    print(f"  Different: {'✓ (GCM random IV)' if enc != enc2 else '✗ (SAME!)'}")

    print("\n" + "=" * 60)
    print("Demo complete. Data encrypted at rest, restored intact.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
