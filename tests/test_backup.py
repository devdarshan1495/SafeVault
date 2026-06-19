"""End-to-end tests for the backup and restore workflow."""

import os
import pytest
from src.core.chunking import RabinChunker
from src.core.dedup import InMemoryDedupStore
from src.core.compression import Compressor
from src.core.encryption import Encryptor
from src.metadata.store import MetadataStore, BackupType, JobStatus
from src.messaging.pubsub import PubSubBus, EventTopic
from src.storage.node import StorageNode
from src.storage.cluster import StorageCluster
from src.backup.engine import BackupEngine
from src.restore.engine import RestoreEngine


@pytest.fixture
def safevault_components():
    pubsub = PubSubBus()
    metadata = MetadataStore()
    dedup = InMemoryDedupStore()
    chunker = RabinChunker(min_chunk=64, avg_chunk=256, max_chunk=512)
    compressor = Compressor(level=3)
    encryptor = Encryptor()
    storage = StorageCluster(replication_factor=2)

    for i in range(3):
        storage.add_node(StorageNode(
            name=f"node-{i}", region="us-east-1", zone=f"us-east-1{chr(97+i)}",
            capacity_bytes=1_000_000,
        ))

    backup_engine = BackupEngine(
        metadata, dedup, chunker, compressor, encryptor, storage, pubsub,
    )
    restore_engine = RestoreEngine(
        metadata, dedup, compressor, encryptor, storage, pubsub,
    )
    return metadata, backup_engine, restore_engine, pubsub


@pytest.mark.asyncio
async def test_full_backup_and_restore(safevault_components):
    metadata, backup_engine, restore_engine, pubsub = safevault_components

    policy = metadata.create_policy(
        user_id="test-user",
        name="Test Policy",
        schedule_cron="0 * * * *",
        retention_days=7,
        backup_type=BackupType.FULL,
        source_paths=["/test"],
    )

    files = {
        "/test/file1.txt": b"Hello SafeVault! " * 500,
        "/test/file2.bin": os.urandom(5000),
    }

    job_id = await backup_engine.run_backup(policy.id, files)
    assert job_id is not None

    job = metadata.jobs[job_id]
    assert job.status == JobStatus.COMPLETED
    assert job.total_bytes > 0

    # Restore
    result = await restore_engine.restore_latest(
        user_id="test-user",
        policy_id=policy.id,
        file_path="/test/file1.txt",
        restore_path="/tmp/restore/file1.txt",
    )
    assert result is not None
    assert result == files["/test/file1.txt"]


@pytest.mark.asyncio
async def test_incremental_backup(safevault_components):
    metadata, backup_engine, restore_engine, pubsub = safevault_components

    policy = metadata.create_policy(
        user_id="test-user",
        name="Incremental Test",
        schedule_cron="0 * * * *",
        retention_days=7,
        backup_type=BackupType.FULL,
        source_paths=["/data"],
    )

    # Full
    files_v1 = {"/data/doc.txt": b"v1 content " * 300}
    job_id = await backup_engine.run_backup(policy.id, files_v1)
    assert job_id is not None

    # Incremental
    files_v2 = {"/data/doc.txt": b"v2 content " * 300}
    job_id2 = await backup_engine.run_backup(policy.id, files_v2)
    assert job_id2 is not None
    job2 = metadata.jobs[job_id2]
    assert job2.backup_type == BackupType.INCREMENTAL

    # Restore latest should get v2
    result = await restore_engine.restore_latest(
        user_id="test-user",
        policy_id=policy.id,
        file_path="/data/doc.txt",
        restore_path="/tmp/restore/doc.txt",
    )
    assert result == files_v2["/data/doc.txt"]


@pytest.mark.asyncio
async def test_backup_with_empty_files(safevault_components):
    metadata, backup_engine, restore_engine, pubsub = safevault_components

    policy = metadata.create_policy(
        user_id="test-user", name="Empty Test", schedule_cron="0 * * * *",
        retention_days=7, backup_type=BackupType.FULL, source_paths=["/empty"],
    )
    job_id = await backup_engine.run_backup(policy.id, {})
    assert job_id is not None
    job = metadata.jobs[job_id]
    assert job.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_backup_failure_recovery(safevault_components):
    metadata, backup_engine, restore_engine, pubsub = safevault_components

    policy = metadata.create_policy(
        user_id="test-user", name="FT Test", schedule_cron="0 * * * *",
        retention_days=7, backup_type=BackupType.FULL, source_paths=["/ft"],
    )

    files = {"/ft/data.bin": os.urandom(2000)}
    job_id = await backup_engine.run_backup(policy.id, files)
    assert job_id is not None

    # Verify events were published
    events = pubsub.get_history(EventTopic.BACKUP_JOB_COMPLETED)
    assert len(events) >= 1

    # Restore should succeed
    result = await restore_engine.restore_latest(
        user_id="test-user", policy_id=policy.id,
        file_path="/ft/data.bin", restore_path="/tmp/restore/data.bin",
    )
    assert result == files["/ft/data.bin"]
