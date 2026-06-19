"""SafeVault CLI — simulate backup, restore, and monitoring workflows."""

import asyncio
import json
import os
import sys
import time
import traceback
from typing import Dict

from src.core.chunking import RabinChunker
from src.core.dedup import InMemoryDedupStore
from src.core.compression import Compressor
from src.core.encryption import Encryptor
from src.metadata.store import MetadataStore, BackupType
from src.messaging.pubsub import PubSubBus, Event, EventTopic
from src.storage.node import StorageNode, NodeStatus
from src.storage.cluster import StorageCluster
from src.backup.scheduler import BackupScheduler
from src.backup.engine import BackupEngine
from src.restore.engine import RestoreEngine
from src.monitoring.metrics import MetricsCollector


class SafeVault:
    """Main application — wires all components together."""

    def __init__(self):
        self.pubsub = PubSubBus()
        self.metadata = MetadataStore()
        self.dedup = InMemoryDedupStore()
        self.chunker = RabinChunker()
        self.compressor = Compressor(level=3)
        self.encryptor = Encryptor()
        self.storage = StorageCluster(replication_factor=3)
        self.scheduler = BackupScheduler(self.metadata)
        self.backup_engine = BackupEngine(
            self.metadata, self.dedup, self.chunker,
            self.compressor, self.encryptor, self.storage, self.pubsub,
        )
        self.restore_engine = RestoreEngine(
            self.metadata, self.dedup, self.compressor,
            self.encryptor, self.storage, self.pubsub,
        )
        self.monitoring = MetricsCollector(self.pubsub)

    def setup_storage_cluster(self):
        """Create a 6-node distributed storage cluster across 2 regions."""
        regions = [
            ("us-east-1", "us-east-1a"),
            ("us-east-1", "us-east-1b"),
            ("us-east-1", "us-east-1c"),
            ("eu-west-1", "eu-west-1a"),
            ("eu-west-1", "eu-west-1b"),
            ("ap-south-1", "ap-south-1a"),
        ]
        for region, zone in regions:
            node = StorageNode(
                name=f"node-{region}-{zone}",
                region=region,
                zone=zone,
                capacity_bytes=1_000_000_000_000,
            )
            self.storage.add_node(node)
        print(f"  Storage cluster: {self.storage.node_count()} nodes online")

    async def demo_full_backup(self):
        """Run a full backup simulation."""
        print("\n" + "=" * 60)
        print("DEMO 1: Full Backup")
        print("=" * 60)

        user_id = "user-001"
        policy = self.metadata.create_policy(
            user_id=user_id,
            name="Daily Backup",
            schedule_cron="0 2 * * *",
            retention_days=30,
            backup_type=BackupType.FULL,
            source_paths=["/data"],
        )
        print(f"  Policy: {policy.name} ({policy.id[:8]}...)")

        self.scheduler.add_policy(policy)

        files = self.backup_engine.simulate_files()
        print(f"  Files to backup: {len(files)} ({sum(len(v) for v in files)} bytes)")

        job_id = await self.backup_engine.run_backup(policy.id, files)
        if job_id:
            job = self.metadata.jobs[job_id]
            print(f"  Backup job: {job_id[:8]}... | Status: {job.status.value}")
            print(f"  Total bytes: {job.total_bytes}")
            print(f"  New chunks: {job.new_chunks_count}")
            print(f"  Dedup savings: {job.dedup_savings_bytes} bytes")
        else:
            print("  ✗ Backup FAILED")
        return policy, files

    async def demo_incremental_backup(self, policy, last_files: Dict[str, bytes]):
        """Run an incremental backup simulation."""
        print("\n" + "=" * 60)
        print("DEMO 2: Incremental Backup")
        print("=" * 60)

        modified = self.backup_engine.simulate_modified_files()
        # Merge: base files + modifications
        files = dict(last_files)
        files.update(modified)

        job_id = await self.backup_engine.run_backup(policy.id, files)
        if job_id:
            job = self.metadata.jobs[job_id]
            print(f"  Incremental job: {job_id[:8]}... | Status: {job.status.value}")
            print(f"  Total bytes: {job.total_bytes}")
            print(f"  New chunks: {job.new_chunks_count}")
            print(f"  Dedup savings: {job.dedup_savings_bytes} bytes")
            print(f"  Type: {job.backup_type.value}")
        else:
            print("  ✗ Incremental backup FAILED")
        return files

    async def demo_restore(self, policy, version_number: int = None):
        """Restore a file from backup."""
        print("\n" + "=" * 60)
        print("DEMO 3: Restore from Backup")
        print("=" * 60)

        try:
            version = self.metadata.get_latest_version(policy.id)
            if not version:
                print("  ✗ No version found")
                return

            result = await self.restore_engine.restore_latest(
                user_id=policy.user_id,
                policy_id=policy.id,
                file_path="/data/document.txt",
                restore_path="/restore/document.txt",
            )
            if result:
                print(f"  Restored file: /data/document.txt ({len(result)} bytes)")
                preview = result[:80]
                print(f"  Preview: {preview!r}...")
            else:
                print("  ✗ Restore returned no data")
        except Exception as e:
            print(f"  ✗ Restore failed: {e}")

    async def demo_monitoring(self):
        """Display monitoring metrics."""
        print("\n" + "=" * 60)
        print("DEMO 4: Monitoring & Observability")
        print("=" * 60)

        summary = self.monitoring.summary()
        print(f"  Backups completed: {summary['backups_completed']}")
        print(f"  Backups failed: {summary['backups_failed']}")
        print(f"  Restores completed: {summary['restores_completed']}")
        print(f"  Restores failed: {summary['restores_failed']}")
        print(f"  Critical alerts: {summary['critical_alerts']}")

        alerts = self.monitoring.get_alerts("critical")
        for alert in alerts:
            print(f"  ⚠ Alert: {alert.message}")

        # Show Pub/Sub event history
        events = self.pubsub.get_history()
        print(f"\n  Pub/Sub events published: {len(events)}")
        for ev in events[-5:]:
            print(f"    [{ev.event_type}] {ev.topic} — {json.dumps(ev.payload)[:60]}")

    async def demo_dedup_efficiency(self):
        """Demonstrate deduplication across two similar files."""
        print("\n" + "=" * 60)
        print("DEMO 5: Deduplication Efficiency")
        print("=" * 60)

        before = self.dedup.total_unique_chunks()
        before_bytes = self.dedup.total_storage_bytes()

        # Two highly similar files
        policy = self.metadata.create_policy(
            "user-002", "Dedup Test", "0 3 * * *",
            30, BackupType.FULL, ["/dedup-test"],
        )
        files = {
            "/dedup-test/file_a.txt": b"ABCDEFGHIJ" * 5000,
            "/dedup-test/file_b.txt": b"ABCDEFGHIJK" * 5000,  # Mostly same prefix
        }
        await self.backup_engine.run_backup(policy.id, files)

        after_unique = self.dedup.total_unique_chunks()
        after_bytes = self.dedup.total_storage_bytes()
        total_raw = sum(len(v) for v in files.values())

        print(f"  Files: 2 ({total_raw} bytes raw)")
        print(f"  Unique chunks stored: {after_unique}")
        print(f"  Storage used: {after_bytes} bytes")
        print(f"  Dedup ratio: {total_raw / max(after_bytes, 1):.2f}x")
        print(f"  Savings: {total_raw - after_bytes} bytes "
              f"({(1 - after_bytes / max(total_raw, 1)) * 100:.1f}%)")

    async def demo_fault_tolerance(self):
        """Demonstrate fault tolerance — node failure and recovery."""
        print("\n" + "=" * 60)
        print("DEMO 6: Fault Tolerance")
        print("=" * 60)

        policy = self.metadata.create_policy(
            "user-003", "FT Test", "0 4 * * *",
            30, BackupType.FULL, ["/ft-test"],
        )
        files = {"/ft-test/important.bin": os.urandom(50_000)}

        # Normal backup
        print("  Normal backup (all nodes online)...")
        job_id = await self.backup_engine.run_backup(policy.id, files)
        print(f"  ✓ Backup complete: {job_id[:8] if job_id else 'FAILED'}")

        # Kill one node
        nodes = list(self.storage._nodes.values())
        if nodes:
            failed_node = nodes[0]
            failed_node.set_status(NodeStatus.OFFLINE)
            print(f"  ✗ Node failed: {failed_node.name}")

            # Restore should still work (quorum read from remaining nodes)
            print("  Restoring despite node failure...")
            try:
                result = await self.restore_engine.restore_latest(
                    user_id="user-003",
                    policy_id=policy.id,
                    file_path="/ft-test/important.bin",
                    restore_path="/restore/important.bin",
                )
                if result and len(result) == 50_000:
                    print(f"  ✓ Restore successful ({len(result)} bytes)")
                else:
                    print(f"  ✗ Restore returned unexpected data")
            except Exception as e:
                print(f"  ✗ Restore failed: {e}")

            # Bring node back
            failed_node.set_status(NodeStatus.ONLINE)
            print(f"  ✓ Node recovered: {failed_node.name}")


async def main():
    print("╔══════════════════════════════════════════════════╗")
    print("║            SafeVault Backup & Restore            ║")
    print("║       Enterprise-Grade Data Protection          ║")
    print("╚══════════════════════════════════════════════════╝")

    app = SafeVault()
    app.setup_storage_cluster()

    # Run demos
    policy, files = await app.demo_full_backup()
    await app.demo_incremental_backup(policy, files)
    await app.demo_restore(policy)
    await app.demo_monitoring()
    await app.demo_dedup_efficiency()
    await app.demo_fault_tolerance()

    print("\n" + "═" * 60)
    print("Demo complete. See docs/ for architecture details.")
    print("═" * 60)


if __name__ == "__main__":
    asyncio.run(main())
