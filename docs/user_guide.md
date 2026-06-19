# SafeVault User Guide

Comprehensive documentation for every SafeVault feature with CLI commands, API references, and operational procedures.

---

## Table of Contents

1. [Installation &amp; Setup](#1-installation--setup)
2. [Configuration Reference](#2-configuration-reference)
3. [Backup Policies](#3-backup-policies)
4. [Full Backup](#4-full-backup)
5. [Incremental Backup](#5-incremental-backup)
6. [Restore Operations](#6-restore-operations)
7. [Storage Cluster Management](#7-storage-cluster-management)
8. [Monitoring &amp; Alerting](#8-monitoring--alerting)
9. [Pub/Sub Event System](#9-pubsub-event-system)
10. [Deduplication](#10-deduplication)
11. [Fault Tolerance &amp; Recovery](#11-fault-tolerance--recovery)
12. [Testing Guide](#12-testing-guide)
13. [API Reference](#13-api-reference)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Installation & Setup

### Prerequisites

- Python 3.12+
- pip (Python package installer)

### Installation

```bash
# Clone the repository
git clone https://github.com/devdarshan1495/SafeVault
cd SafeVault

# Install dependencies
pip install -r requirements.txt
```

### Verify Installation

```bash
# Run the full demo (6 scenarios)
python3 -m src.main

# Run the test suite
python3 -m pytest tests/ -v
```

Expected output from `python3 -m src.main`:

```
╔══════════════════════════════════════════════════╗
║            SafeVault Backup & Restore            ║
║       Enterprise-Grade Data Protection          ║
╚══════════════════════════════════════════════════╝
  Storage cluster: 6 nodes online

============================================================
DEMO 1: Full Backup
============================================================
  Policy: Daily Backup (abc12345...)
  Files to backup: 5 (310157 bytes)
  Backup job: def67890... | Status: completed
  Total bytes: 310157
  New chunks: 142
  Dedup savings: 287654 bytes
...
```

---

## 2. Configuration Reference

### Storage Cluster Configuration

The storage cluster is configured with 6 nodes across 3 regions:

| Region         | Zones                              | Nodes | Capacity  |
| -------------- | ---------------------------------- | ----- | --------- |
| `us-east-1`  | us-east-1a, us-east-1b, us-east-1c | 3     | 1 TB each |
| `eu-west-1`  | eu-west-1a, eu-west-1b             | 2     | 1 TB each |
| `ap-south-1` | ap-south-1a                        | 1     | 1 TB each |

Configured in `src/main.py:47-65`:

```python
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
```

### Replication Settings

Configured in `src/storage/cluster.py:11`:

```python
class StorageCluster:
    def __init__(self, replication_factor: int = 3):
```

Default: 3 replicas per chunk (2 in primary region, 1 cross-region). Quorum for writes: `N/2 + 1 = 2`.

### Chunker Configuration

Configured in `src/core/chunking.py:18-27`:

| Parameter     | Default     | Description                 |
| ------------- | ----------- | --------------------------- |
| `min_chunk` | 4096 bytes  | Minimum chunk size          |
| `avg_chunk` | 8192 bytes  | Average chunk size (target) |
| `max_chunk` | 16384 bytes | Maximum chunk size          |

### Compression Configuration

Configured in `src/core/compression.py:8`:

```python
class Compressor:
    def __init__(self, level: int = 3, enable: bool = True):
```

- Level range: 1 (fastest) to 22 (best compression)
- Can be disabled by passing `enable=False`

### Encryption Configuration

Configured in `src/core/encryption.py:17-21`:

```python
class Encryptor:
    def __init__(self, key: bytes = None, enable: bool = True):
```

- Auto-generates a 256-bit key if none provided
- AES-256-GCM mode with 96-bit IV and 128-bit authentication tag
- Can derive key from password via `Encryptor.from_password(password)`

---

## 3. Backup Policies

### Creating a Policy

Policies define backup schedules, retention, and source paths.

**Python API:**

```python
from src.metadata.store import MetadataStore, BackupType

metadata = MetadataStore()
policy = metadata.create_policy(
    user_id="user-001",
    name="Daily Backup",
    schedule_cron="0 2 * * *",       # Daily at 2 AM
    retention_days=30,                # Keep for 30 days
    backup_type=BackupType.FULL,      # or BackupType.INCREMENTAL
    source_paths=["/data", "/home"],  # Directories to back up
)
print(f"Policy ID: {policy.id}")
```

### Policy Parameters

| Field              | Type         | Description                      |
| ------------------ | ------------ | -------------------------------- |
| `id`             | UUID string  | Auto-generated unique identifier |
| `user_id`        | string       | Owner of the policy              |
| `name`           | string       | Human-readable name              |
| `schedule_cron`  | string       | 5-field cron expression          |
| `retention_days` | int          | Number of days to retain backups |
| `backup_type`    | BackupType   | `FULL` or `INCREMENTAL`      |
| `source_paths`   | List[string] | Directories to include           |
| `enabled`        | bool         | Whether policy is active         |
| `created_at`     | float        | Unix timestamp                   |

### Scheduling a Policy

**Python API:**

```python
from src.backup.scheduler import BackupScheduler

scheduler = BackupScheduler(metadata)
entry = scheduler.add_policy(policy)
print(f"Next run: {entry.next_run}")
print(f"Interval: {entry.interval_seconds}s")
```

### Checking Due Schedules

**Python API:**

```python
due_policy_ids = scheduler.check_due()
for pid in due_policy_ids:
    print(f"Policy {pid} is due for backup")
```

### Running the Scheduler Loop

**Python API:**

```python
async def on_backup_due(policy_id):
    print(f"Backup triggered for policy {policy_id}")

scheduler.on_backup_due(on_backup_due)
await scheduler.run_loop(interval_sec=1.0)

# Later:
scheduler.stop()
```

---

## 4. Full Backup

### Executing a Full Backup

Full backups process all files through the complete pipeline: chunk → dedup → compress → encrypt → store.

**Python API:**

```python
from src.backup.engine import BackupEngine

engine = BackupEngine(
    metadata=metadata_store,
    dedup=dedup_store,
    chunker=rabin_chunker,
    compressor=compressor,
    encryptor=encryptor,
    storage_cluster=storage_cluster,
    pubsub=pubsub_bus,
)

files = {
    "/data/document.txt": b"Hello World! " * 1000,
    "/data/database.dump": os.urandom(100_000),
}

job_id = await engine.run_backup(policy_id, files)
```

### Pipeline Steps

1. **Chunking** — File is split into variable-size chunks using Rabin fingerprint CDC
2. **Dedup** — Each chunk's SHA-256 hash is checked against the global index; duplicates increment refcount and are skipped
3. **Compression** — New chunks are compressed with zstandard (level 3)
4. **Encryption** — Compressed chunks are encrypted with AES-256-GCM
5. **Storage** — Encrypted chunks are written to 3 storage nodes via quorum write

### Monitoring Backup Progress

The backup engine publishes events at each lifecycle stage:

| Event     | Topic                    | Payload                                                              |
| --------- | ------------------------ | -------------------------------------------------------------------- |
| Started   | `backup.job.started`   | `{job_id, policy_id, type}`                                        |
| Completed | `backup.job.completed` | `{job_id, version_id, files, bytes, dedup_savings, unique_chunks}` |
| Failed    | `backup.job.failed`    | `{job_id, policy_id, error}`                                       |

### Simulating Files for Testing

```python
# Generate 5 test files
files = engine.simulate_files()
# Returns: {"/data/document.txt": ..., "/data/database.dump": ..., etc.}
```

---

## 5. Incremental Backup

### How Incremental Backup Works

The backup engine automatically detects whether a backup should be incremental or full based on existing versions:

```python
# src/backup/engine.py:56-62
last_version = self.metadata.get_latest_version(policy_id)
backup_type = BackupType.FULL
parent_version_id = None

if last_version:
    backup_type = BackupType.INCREMENTAL
    parent_version_id = last_version.id
```

- **First run**: Always a full backup
- **Subsequent runs**: Automatically incremental
- Only new/modified chunks are stored; unchanged chunks reference existing ones

### Executing an Incremental Backup

**Python API:**

```python
# After a full backup has run:
modified_files = engine.simulate_modified_files()
# Returns modified versions of existing files + a new file

job_id = await engine.run_backup(policy_id, modified_files)
```

### Version Chain

Each backup creates a version linked to its parent:

```
Version 1 (FULL) ← Version 2 (INCREMENTAL) ← Version 3 (INCREMENTAL)
```

The version chain is used during restore to reconstruct files by walking from the latest version back to the last full backup.

### Simulating Modified Files

```python
modified = engine.simulate_modified_files()
# - /data/document.txt: slightly modified (1000→1001 repeats)
# - /data/database.dump: completely changed
# - /data/new_file.csv: new file
```

---

## 6. Restore Operations

### Restoring the Latest Version

Restores the most recent version of a file from the version chain.

**Python API:**

```python
from src.restore.engine import RestoreEngine

restore_engine = RestoreEngine(
    metadata=metadata_store,
    dedup=dedup_store,
    compressor=compressor,
    encryptor=encryptor,
    storage_cluster=storage_cluster,
    pubsub=pubsub_bus,
)

result = await restore_engine.restore_latest(
    user_id="user-001",
    policy_id=policy_id,
    file_path="/data/document.txt",
    restore_path="/restore/document.txt",
)

# Returns the reconstructed file content as bytes
print(f"Restored {len(result)} bytes")
```

### Point-in-Time Restore

Restores a file as it existed at a specific timestamp. Finds the latest version at or before the target time.

**Python API:**

```python
import time

target_time = time.time() - 86400  # 1 day ago

result = await restore_engine.restore_point_in_time(
    user_id="user-001",
    policy_id=policy_id,
    file_path="/data/document.txt",
    target_timestamp=target_time,
    restore_path="/restore/document_old.txt",
)
```

### Version Chain Traversal

The restore engine walks the version chain from the selected version back to the last full backup (`src/restore/engine.py:137-174`):

```python
current = version
while current is not None:
    file_chunks = self.metadata.get_file_chunks(file_id)
    for fc in file_chunks:
        if fc.chunk_index not in all_chunks:
            all_chunks[fc.chunk_index] = fc
        elif current.backup_type == BackupType.INCREMENTAL:
            all_chunks[fc.chunk_index] = fc  # Override with newer

    if current.backup_type == BackupType.FULL:
        break  # Stop at full backup base
    current = self.metadata.versions.get(current.parent_version_id)
```

- Incremental versions override chunks for the same index
- Full version provides the base snapshot
- Chunks are reassembled in sorted index order

### Integrity Verification

After reconstruction, the restore engine verifies SHA-256 checksum (`src/restore/engine.py:100-103`):

```python
import hashlib
if hashlib.sha256(result).hexdigest() != file_meta.file_hash:
    raise RuntimeError(f"Integrity check failed for {file_path}")
```

### Chunk Retrieval

Chunks are retrieved with a two-tier lookup (`src/restore/engine.py:176-192`):

1. Check in-memory dedup store (fast path)
2. Fall back to storage cluster quorum read
3. Decrypt → decompress → return data

### Restore Events

| Event     | Topic                     | Payload                                    |
| --------- | ------------------------- | ------------------------------------------ |
| Started   | `restore.job.started`   | `{job_id, version_id, file_path}`        |
| Completed | `restore.job.completed` | `{job_id, version_id, file_path, bytes}` |
| Failed    | `restore.job.failed`    | `{job_id, version_id, error}`            |

---

## 7. Storage Cluster Management

### Cluster Overview

The `StorageCluster` class manages distributed storage nodes with quorum-based replication.

**Initialization:**

```python
from src.storage.cluster import StorageCluster

cluster = StorageCluster(replication_factor=3)
```

### Adding Nodes

```python
from src.storage.node import StorageNode, NodeStatus

node = StorageNode(
    name="node-us-east-1a",
    region="us-east-1",
    zone="us-east-1a",
    capacity_bytes=1_000_000_000_000,  # 1 TB
    status=NodeStatus.ONLINE,
)
cluster.add_node(node)
```

### Node Status

| Status          | Description                  |
| --------------- | ---------------------------- |
| `ONLINE`      | Node is operational          |
| `OFFLINE`     | Node is unreachable/down     |
| `DEGRADED`    | Node is partially functional |
| `MAINTENANCE` | Node is under maintenance    |

### Reading/Writing with Quorum

**Quorum Write** (`src/storage/cluster.py:53-81`):

```python
locations = cluster.quorum_write(chunk_hash, encrypted_data)
# Returns list of (node_id, path) tuples
```

- Selects 2 nodes from primary region + 1 from another region
- Requires at least quorum (`N/2 + 1 = 2`) successful writes
- Rolls back all writes if quorum is not met

**Quorum Read** (`src/storage/cluster.py:83-92`):

```python
data = cluster.quorum_read(chunk_hash)
```

- Iterates through online nodes until data is found
- Returns data from first successful node

### Node Selection Strategy

```python
# src/storage/cluster.py:31-51
def select_nodes_for_write(self, count=None, primary_region="us-east-1"):
    primary = [n for n in candidates if n.region == primary_region]
    other = [n for n in candidates if n.region != primary_region]
    selected = random.sample(primary, min(2, len(primary)))
    if len(selected) < count and other:
        selected.append(random.choice(other))
```

### Node Capacity

```python
node = cluster.get_node(node_id)
print(f"Used: {node.used_bytes} bytes")
print(f"Available: {node.available_bytes} bytes")
print(f"Chunks stored: {node.chunk_count()}")
```

### Storage Node Operations

```python
from src.storage.node import StorageNode, NodeStatus

# Change node status
node.set_status(NodeStatus.OFFLINE)    # Simulate failure
node.set_status(NodeStatus.ONLINE)     # Bring back online
node.set_status(NodeStatus.MAINTENANCE)  # Maintenance mode

# Check node status
is_online = node.status == NodeStatus.ONLINE
has_chunk = node.has_chunk(chunk_hash)
```

---

## 8. Monitoring & Alerting

### Metrics Collector

The `MetricsCollector` subscribes to all Pub/Sub events and aggregates metrics.

**Initialization:**

```python
from src.monitoring.metrics import MetricsCollector

monitoring = MetricsCollector(pubsub_bus)
```

### Subscribed Event Topics

| Topic                      | Handler                  | Metrics Updated                                                                  |
| -------------------------- | ------------------------ | -------------------------------------------------------------------------------- |
| `backup.job.completed`   | `_on_backup_complete`  | `backup.completed` counter, `backup.bytes`, `backup.dedup_savings` samples |
| `backup.job.failed`      | `_on_backup_failed`    | `backup.failed` counter, critical alert                                        |
| `restore.job.completed`  | `_on_restore_complete` | `restore.completed` counter                                                    |
| `restore.job.failed`     | `_on_restore_failed`   | `restore.failed` counter, critical alert                                       |
| `storage.node.heartbeat` | `_on_heartbeat`        | `storage.node.heartbeat` sample                                                |

### Retrieving Metrics

```python
# Get summary
summary = monitoring.summary()
print(summary)
# {
#     "backups_completed": 2,
#     "backups_failed": 0,
#     "restores_completed": 1,
#     "restores_failed": 0,
#     "critical_alerts": 0,
#     "total_samples": 12
# }

# Get specific counter
completed = monitoring.get_counter("backup.completed")

# Get alerts
all_alerts = monitoring.get_alerts()
critical = monitoring.get_alerts("critical")
for alert in critical:
    print(f"⚠ {alert.severity}: {alert.message}")
```

### Working with Metric Samples

```python
from src.monitoring.metrics import MetricSample

sample = MetricSample(
    name="backup.bytes",
    value=310157,
    labels={"policy_id": "abc-123"},
)
```

---

## 9. Pub/Sub Event System

### Overview

SafeVault uses an asynchronous Pub/Sub event bus for loose coupling between components. In production, this would be backed by Apache Kafka or RabbitMQ.

### Event Topics

| Topic                         | Producer       | Consumers            | Description             |
| ----------------------------- | -------------- | -------------------- | ----------------------- |
| `scheduler.backup.due`      | Scheduler      | Backup Engine        | Policy due for backup   |
| `backup.job.started`        | Backup Engine  | Monitoring, Metadata | Backup job started      |
| `backup.job.completed`      | Backup Engine  | Monitoring, Metadata | Backup job completed    |
| `backup.job.failed`         | Backup Engine  | Monitoring, Metadata | Backup job failed       |
| `backup.chunk.stored`       | Backup Engine  | Monitoring           | Individual chunk stored |
| `restore.job.started`       | Restore Engine | Monitoring, Metadata | Restore job started     |
| `restore.job.completed`     | Restore Engine | Monitoring, Metadata | Restore job completed   |
| `restore.job.failed`        | Restore Engine | Monitoring, Metadata | Restore job failed      |
| `storage.node.heartbeat`    | Storage Nodes  | Monitoring           | Node heartbeat signal   |
| `storage.node.failed`       | Storage Nodes  | Monitoring, Cluster  | Node failure detected   |
| `replication.sync.required` | Cluster        | Storage Nodes        | Replication sync needed |
| `monitoring.alert`          | Monitoring     | Alerting System      | Alert generated         |

### Publishing Events

```python
from src.messaging.pubsub import PubSubBus, Event, EventTopic

bus = PubSubBus()

event = Event(
    topic=EventTopic.BACKUP_JOB_COMPLETED,
    event_type="full",
    payload={"job_id": "abc123", "bytes": 100000},
    producer="backup-engine",
)

await bus.publish(event)
```

### Subscribing to Events

```python
async def on_backup_complete(event: Event):
    print(f"Backup complete: {event.payload}")

bus.subscribe(EventTopic.BACKUP_JOB_COMPLETED, on_backup_complete)
```

### Event History

```python
# All events
all_events = bus.get_history()

# Filter by topic
backup_events = bus.get_history(topic=EventTopic.BACKUP_JOB_STARTED)

for ev in all_events:
    print(f"[{ev.event_type}] {ev.topic} — {ev.payload}")
```

### Unsubscribing

```python
bus.unsubscribe(EventTopic.BACKUP_JOB_COMPLETED, on_backup_complete)
```

---

## 10. Deduplication

### How Dedup Works

1. Each chunk is hashed with SHA-256
2. Hash is checked against a Bloom filter (fast probabilistic check)
3. If the chunk exists, the reference count is incremented
4. If the chunk is new, it's added to the store along with the Bloom filter

### Dedup Store API

```python
from src.core.dedup import InMemoryDedupStore

dedup = InMemoryDedupStore(enable_bloom=True)

# Store a chunk (returns SHA-256 hash)
chunk_hash = dedup.store(chunk_data)

# Check if a chunk might exist
exists = dedup.might_exist(chunk_hash)

# Look up a chunk by hash
data = dedup.lookup(chunk_hash)

# Get reference count
count = dedup.ref_count(chunk_hash)

# Release a chunk (decrements refcount)
dedup.release(chunk_hash)

# Get statistics
unique = dedup.total_unique_chunks()
storage = dedup.total_storage_bytes()
```

### Measuring Dedup Efficiency

```python
# Before
unique_before = dedup.total_unique_chunks()
storage_before = dedup.total_storage_bytes()

# Process two similar files
files = {
    "file_a.txt": b"ABCDEFGHIJ" * 5000,
    "file_b.txt": b"ABCDEFGHIJK" * 5000,
}

# After
unique_after = dedup.total_unique_chunks()
storage_after = dedup.total_storage_bytes()

# Metrics
total_raw = sum(len(v) for v in files.values())
dedup_ratio = total_raw / max(storage_after, 1)
savings_pct = (1 - storage_after / max(total_raw, 1)) * 100

print(f"Dedup ratio: {dedup_ratio:.2f}x")
print(f"Savings: {savings_pct:.1f}%")
```

### Dedup in the Backup Pipeline

Reference counting is managed in `src/backup/engine.py:168-170`:

```python
# Dedup
chunk_hash = self.dedup.store(chunk_data)
is_new = self.dedup.ref_count(chunk_hash) == 1
```

- `is_new = True`: First occurrence, goes through compress → encrypt → store
- `is_new = False`: Duplicate, only the reference count is updated

---

## 11. Fault Tolerance & Recovery

### Simulating Node Failures

```python
from src.storage.node import StorageNode, NodeStatus

# Mark node as offline (simulates crash/network partition)
failed_node.set_status(NodeStatus.OFFLINE)

# Verify cluster continues operating
# - Writes are routed to remaining online nodes
# - Reads use quorum_read (any online node)

# Restore the node
failed_node.set_status(NodeStatus.ONLINE)
```

### Quorum-Based Resilience

With replication factor 3 and 6 nodes:

| Scenario                      | Nodes Down | Can Write?       | Can Read?        |
| ----------------------------- | ---------- | ---------------- | ---------------- |
| No failures                   | 0          | Yes (3/3)        | Yes              |
| Single node                   | 1          | Yes (≥2 quorum) | Yes              |
| Two nodes (same region)       | 2          | Yes (≥2 quorum) | Yes              |
| Two nodes (different regions) | 2          | Depends          | Yes              |
| Three nodes                   | 3          | No (<2 quorum)   | Yes (any online) |

### Writing Despite Failures

```python
# src/storage/cluster.py:53-81
def quorum_write(self, chunk_hash, data, region="us-east-1"):
    nodes = self.select_nodes_for_write(primary_region=region)
    success_count = 0
    quorum = self.replication_factor // 2 + 1  # = 2

    for node in nodes:
        try:
            node.store(chunk_hash, data)
            success_count += 1
        except Exception:
            pass  # Failed writes are tracked

    if success_count < quorum:
        # Rollback all writes from this operation
        raise RuntimeError("Quorum write failed")
```

### Reading Despite Failures

```python
# src/storage/cluster.py:83-92
def quorum_read(self, chunk_hash):
    for node in self.online_nodes():
        try:
            data = node.retrieve(chunk_hash)
            if data is not None:
                return data
        except Exception:
            continue
    return None
```

Checking SHA-256 checksums on each read (`src/storage/node.py:70-75`):

```python
data_hash = self._compute_hash(chunk.data)
if data_hash != chunk.hash:
    raise RuntimeError(f"Checksum mismatch for chunk {chunk_hash}")
```

### Full Fault Tolerance Demo

```python
async def demo_fault_tolerance():
    # 1. Normal backup with all nodes online
    job_id = await engine.run_backup(policy_id, files)

    # 2. Kill a node
    failed_node.set_status(NodeStatus.OFFLINE)

    # 3. Restore still works (reads from remaining nodes)
    result = await restore_engine.restore_latest(...)

    # 4. Recover node
    failed_node.set_status(NodeStatus.ONLINE)
```

---

## 12. Testing Guide

### Running All Tests

```bash
python3 -m pytest tests/ -v
```

Expected: 57 tests passed.

### Running Specific Test Files

```bash
# Test chunking
python3 -m pytest tests/test_chunking.py -v

# Test deduplication
python3 -m pytest tests/test_dedup.py -v

# Test compression
python3 -m pytest tests/test_compression.py -v

# Test encryption
python3 -m pytest tests/test_encryption.py -v

# Test backup engine
python3 -m pytest tests/test_backup.py -v

# Test restore engine (if available)
python3 -m pytest tests/test_restore.py -v

# Test metadata store
python3 -m pytest tests/test_metadata.py -v

# Test Pub/Sub
python3 -m pytest tests/test_pubsub.py -v

# Test scheduler
python3 -m pytest tests/test_scheduler.py -v

# Test storage node and cluster
python3 -m pytest tests/test_storage.py -v
```

### Running with Coverage

```bash
pip install pytest-cov
python3 -m pytest tests/ -v --cov=src
```

### Test Categories

| Test File               | Tests                       | What It Covers                                                                         |
| ----------------------- | --------------------------- | -------------------------------------------------------------------------------------- |
| `test_chunking.py`    | RabinChunker, FixedChunker  | Boundary detection, min/max chunk enforcement, empty data                              |
| `test_dedup.py`       | InMemoryDedupStore          | Hash computation, dedup detection, ref counting, Bloom filter, release                 |
| `test_compression.py` | Compressor                  | Round-trip compress/decompress, level parameter, disable flag, error handling          |
| `test_encryption.py`  | Encryptor                   | Round-trip encrypt/decrypt, key derivation, different key failure, disable flag        |
| `test_backup.py`      | BackupEngine                | Full backup pipeline, incremental backup (if implemented), event publishing            |
| `test_metadata.py`    | MetadataStore               | CRUD for policies, files, jobs, versions, chunks, locations, restore jobs              |
| `test_pubsub.py`      | PubSubBus                   | Publish/subscribe, async subscribers, history, unsubscribe, error handling             |
| `test_scheduler.py`   | BackupScheduler             | Policy scheduling, cron parsing, due checking, run loop, event emission                |
| `test_storage.py`     | StorageNode, StorageCluster | Store/retrieve/delete, node status, capacity, checksum verification, quorum write/read |

---

## 13. API Reference

### SafeVault Application Class

**File:** `src/main.py`

```python
class SafeVault:
    def __init__(self)
    def setup_storage_cluster(self)
    async def demo_full_backup(self)
    async def demo_incremental_backup(self, policy, last_files)
    async def demo_restore(self, policy, version_number=None)
    async def demo_monitoring(self)
    async def demo_dedup_efficiency(self)
    async def demo_fault_tolerance(self)
```

### Core Components

| Component   | File                        | Class                  | Key Methods                                                                                                                                                                                          |
| ----------- | --------------------------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Chunking    | `src/core/chunking.py`    | `RabinChunker`       | `chunk(data) → List[(offset, size)]`                                                                                                                                                              |
| Chunking    | `src/core/chunking.py`    | `FixedChunker`       | `chunk(data) → List[(offset, size)]`                                                                                                                                                              |
| Dedup       | `src/core/dedup.py`       | `InMemoryDedupStore` | `store(data) → hash`, `lookup(hash) → bytes`, `release(hash)`, `ref_count(hash) → int`, `might_exist(hash) → bool`, `total_unique_chunks() → int`, `total_storage_bytes() → int` |
| Compression | `src/core/compression.py` | `Compressor`         | `compress(data) → bytes`, `decompress(data) → bytes`                                                                                                                                           |
| Encryption  | `src/core/encryption.py`  | `Encryptor`          | `encrypt(plaintext) → (ciphertext, iv)`, `decrypt(data, iv) → bytes`, `from_password(password, salt) → Encryptor`                                                                           |

### Backup Components

| Component     | File                        | Class               | Key Methods                                                                                               |
| ------------- | --------------------------- | ------------------- | --------------------------------------------------------------------------------------------------------- |
| Backup Engine | `src/backup/engine.py`    | `BackupEngine`    | `run_backup(policy_id, files) → Optional[job_id]`                                                      |
| Scheduler     | `src/backup/scheduler.py` | `BackupScheduler` | `add_policy(policy) → ScheduleEntry`, `check_due() → List[str]`, `run_loop(interval)`, `stop()` |

### Restore Components

| Component      | File                      | Class             | Key Methods                                                                                                                                                                                     |
| -------------- | ------------------------- | ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Restore Engine | `src/restore/engine.py` | `RestoreEngine` | `restore_latest(user_id, policy_id, file_path, restore_path) → Optional[bytes]`, `restore_point_in_time(user_id, policy_id, file_path, target_timestamp, restore_path) → Optional[bytes]` |

### Storage Components

| Component       | File                       | Class              | Key Methods                                                                                                                                                                                                        |
| --------------- | -------------------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Storage Node    | `src/storage/node.py`    | `StorageNode`    | `store(hash, data) → bool`, `retrieve(hash) → Optional[bytes]`, `delete(hash) → bool`, `has_chunk(hash) → bool`, `chunk_count() → int`, `set_status(status)`, `used_bytes`, `available_bytes` |
| Storage Cluster | `src/storage/cluster.py` | `StorageCluster` | `add_node(node)`, `remove_node(node_id)`, `online_nodes(region?) → List`, `quorum_write(hash, data, region?) → List[(node_id, path)]`, `quorum_read(hash) → Optional[bytes]`, `node_count() → int` |

### Metadata Components

| Component      | File                      | Class             | Key Methods                                                                                                                                                                                                                                                                                                                                                                         |
| -------------- | ------------------------- | ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Metadata Store | `src/metadata/store.py` | `MetadataStore` | `create_policy(...)`, `get_policy(id)`, `register_file(...)`, `get_file_by_path(path)`, `create_job(...)`, `update_job_status(...)`, `create_version(...)`, `get_latest_version(policy_id)`, `register_chunk(...)`, `add_chunk_location(...)`, `link_file_chunk(...)`, `get_file_chunks(file_id)`, `create_restore_job(...)`, `update_restore_job(...)` |

### Messaging Components

| Component | File                        | Class         | Key Methods                                                                                                                               |
| --------- | --------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Pub/Sub   | `src/messaging/pubsub.py` | `PubSubBus` | `publish(event)`, `subscribe(topic, callback)`, `unsubscribe(topic, callback)`, `get_history(topic?) → List[Event]`, `clear()` |

### Monitoring Components

| Component  | File                          | Class                | Key Methods                                                                                   |
| ---------- | ----------------------------- | -------------------- | --------------------------------------------------------------------------------------------- |
| Monitoring | `src/monitoring/metrics.py` | `MetricsCollector` | `summary() → dict`, `get_counter(name) → int`, `get_alerts(severity?) → List[Alert]` |

### Enums

| Enum              | Values                                                             | Defined In                  |
| ----------------- | ------------------------------------------------------------------ | --------------------------- |
| `JobStatus`     | `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED` | `src/metadata/store.py`   |
| `VersionStatus` | `ACTIVE`, `EXPIRED`, `DELETED`                               | `src/metadata/store.py`   |
| `BackupType`    | `FULL`, `INCREMENTAL`                                          | `src/metadata/store.py`   |
| `NodeStatus`    | `ONLINE`, `OFFLINE`, `DEGRADED`, `MAINTENANCE`             | `src/storage/node.py`     |
| `EventTopic`    | 12 topics (see Pub/Sub section)                                    | `src/messaging/pubsub.py` |

---

## 14. Troubleshooting

### Common Issues

| Symptom                     | Likely Cause                     | Solution                                                                            |
| --------------------------- | -------------------------------- | ----------------------------------------------------------------------------------- |
| `Quorum write failed`     | Too many nodes offline           | Check `NodeStatus` of cluster nodes; bring nodes back online                      |
| `Checksum mismatch`       | Data corruption detected         | Chunk will be re-read from another replica; if persistent, restore from full backup |
| `Policy not found`        | Invalid policy_id                | Verify policy exists via `metadata.get_policy(id)`                                |
| `No versions found`       | No backup ever ran               | Run a full backup first                                                             |
| `Integrity check failed`  | File hash mismatch after restore | Chunks may be corrupted; restore from a different version                           |
| `Node is offline`         | Node status set to OFFLINE       | Call `node.set_status(NodeStatus.ONLINE)`                                         |
| `Not enough online nodes` | Cluster has insufficient nodes   | Add more nodes or bring existing ones online                                        |
| `Module not found`        | Missing dependency               | Run `pip install -r requirements.txt`                                             |

### Debugging Tips

**Enable verbose Pub/Sub logging:**

```python
pubsub = PubSubBus()

# Track all events
original_publish = pubsub.publish
async def logging_publish(event):
    print(f"[EVENT] {event.topic}: {event.payload}")
    await original_publish(event)
pubsub.publish = logging_publish
```

**Inspect metadata state:**

```python
# After backup
print(f"Policies: {len(metadata.policies)}")
print(f"Jobs: {len(metadata.jobs)}")
print(f"Versions: {len(metadata.versions)}")
print(f"Files: {len(metadata.files)}")
print(f"Chunks: {len(metadata.chunks)}")
print(f"Chunk locations: {sum(len(v) for v in metadata.chunk_locations.values())}")
```

**Inspect storage nodes:**

```python
for nid, node in cluster._nodes.items():
    print(f"{node.name}: {node.status.value}, "
          f"{node.chunk_count()} chunks, "
          f"{node.used_bytes / 1024:.1f} KB used")
```

### Getting Help

- Architecture docs: `docs/architecture.md`
- Database schema: `docs/database_schema.md`
- Algorithm reference: `docs/algorithms.md`
- GitHub issues: https://github.com/`<your-username>`/SafeVault/issues
