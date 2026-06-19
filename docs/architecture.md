# SafeVault — System Architecture

## 1. High-Level Architecture

```
                         ┌──────────────────────────────────────────────────┐
                         │                    Clients                       │
                         │  (CLI / SDK / API Gateway / Web UI)             │
                         └──────────────┬───────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                            API Gateway Layer                                 │
│         Rate Limiting · Auth (OAuth2/IAM) · Request Validation              │
└──────┬───────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Orchestration Layer                                  │
├───────────────────┬───────────────────┬───────────────────┬─────────────────┤
│  Backup Scheduler │   Backup Engine   │   Restore Engine  │  Metadata Svc   │
│  (Cron / Quartz)  │  (Job Orchestr.)  │ (Recovery Coord.) │ (Catalog + I/F) │
└────────┬──────────┴────────┬──────────┴─────────┬─────────┴────────┬────────┘
         │                   │                    │                  │
         ▼                   ▼                    ▼                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Event Bus (Pub/Sub)                                   │
│  Topics: backup.jobs.* · restore.jobs.* · replication.* · monitoring.*      │
│  storage.node.* · metadata.* · scheduler.*                                  │
└──────┬───────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Processing Layer                                     │
├───────────────┬───────────────┬───────────────┬───────────────┬──────────────┤
│  Chunker      │  Dedup Engine │  Compressor   │  Encryptor    │  Integrity   │
│  (CDC/Rabin)  │  (SHA-256)    │  (Zstandard)  │  (AES-256)    │  (SHA-512)   │
├───────────────┴───────────────┴───────────────┴───────────────┴──────────────┤
│                         Pipeline: Chunk → Hash → Dedup → Compress → Encrypt  │
└──────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Storage Layer                                        │
├───────────────────────────────┬──────────────────────────────────────────────┤
│     Metadata Database         │         Distributed Storage Cluster          │
│  ┌─────────────────────────┐  │  ┌──────────┐ ┌──────────┐ ┌──────────┐     │
│  │ PostgreSQL (Relational) │  │  │ Storage  │ │ Storage  │ │ Storage  │     │
│  │  users, schedules, jobs │  │  │ Node A   │ │ Node B   │ │ Node C   │     │
│  │  versions, restore_jobs │  │  │ (Zone 1) │ │ (Zone 2) │ │ (Zone 3) │     │
│  ├─────────────────────────┤  │  └──────────┘ └──────────┘ └──────────┘     │
│  │ Redis (Cache / Queues)  │  │  │ Replica  │ │ Replica  │ │ Replica  │     │
│  │  session, locks, temp   │  │  │Zone 1b   │ │Zone 2b   │ │Zone 3b   │     │
│  └─────────────────────────┘  │  └──────────┘ └──────────┘ └──────────┘     │
└───────────────────────────────┴──────────────────────────────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                     Monitoring & Observability                               │
│  Prometheus + Grafana · Structured Logging (ELK) · Distributed Tracing       │
│  Alerts: backup_failure, node_down, replication_lag, quota_exceeded          │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Complete Component Descriptions

### 2.1 API Gateway Layer

**File:** `src/main.py:41` (simulated as `SafeVault.setup_storage_cluster`)

The API Gateway is the sole entry point for all client interactions. In simulation, the `SafeVault` class in `main.py` serves as this entry point. In production:

| Feature | Description |
|---------|-------------|
| Authentication | OAuth 2.0 / IAM token validation before any operation |
| Rate Limiting | Per-user and per-IP throttling (100 req/s standard, 1000 req/s enterprise) |
| Request Validation | Schema validation, size limits, content-type checks |
| Routing | Routes to Backup Scheduler, Backup Engine, Restore Engine, or Metadata Service |
| TLS Termination | All traffic encrypted via TLS 1.3 |

---

### 2.2 Orchestration Layer

#### 2.2.1 Backup Scheduler

**File:** `src/backup/scheduler.py`

The Backup Scheduler manages cron-like schedules for periodic backups.

**Key classes and methods:**

| Method | Description |
|--------|-------------|
| `BackupScheduler.__init__(metadata_store)` | Initializes scheduler with metadata store reference |
| `add_policy(policy)` | Registers a backup policy with its cron schedule |
| `check_due()` | Returns list of policy_ids whose schedules are due |
| `on_backup_due(listener)` | Registers a callback for when backups are due |
| `run_loop(interval_sec)` | Async loop that checks due schedules at regular intervals |
| `stop()` | Stops the scheduler loop |

**Schedule evaluation logic:**

The cron parser (`parse_cron_simple`) supports standard 5-field cron expressions:
- `0 2 * * *` → Run daily at 2 AM
- `0 */6 * * *` → Run every 6 hours
- `0 2 * * 0` → Run every Sunday at 2 AM
- `* * * * *` → Run every minute

The scheduler computes the `interval_seconds` from the cron expression:
- If hour field is `*/N`, interval = N × 3600 seconds
- If minute field is `*/N` and < 60, interval = N × 60 seconds
- Default interval = 3600 seconds (1 hour)

---

#### 2.2.2 Backup Engine

**File:** `src/backup/engine.py`

The Backup Engine orchestrates a single backup job from start to finish.

**Key methods:**

| Method | Description |
|--------|-------------|
| `run_backup(policy_id, source_files)` | Executes a full or incremental backup for a policy |
| `_process_file(file_path, content, policy, version, job)` | Processes one file through the pipeline |
| `simulate_files(base_path)` | Generates test files (5 files: document.txt, database.dump, config.json, image.png, log.txt) |
| `simulate_modified_files()` | Generates modified test files for incremental backup testing |

**Backup type determination:**

```
if no previous version exists  → FULL backup
if previous version exists     → INCREMENTAL backup (parent = last version)
```

**Backup job lifecycle:**

```
PENDING → RUNNING → COMPLETED (or FAILED)
```

During execution:
1. Creates a `BackupJob` record with status = PENDING
2. Updates status to RUNNING with `started_at` timestamp
3. Publishes `BACKUP_JOB_STARTED` event to Pub/Sub
4. Creates a `BackupVersion` record (version_number = last + 1)
5. Iterates over all source files:
   a. Computes SHA-256 hash of the full file
   b. Registers/updates file metadata
   c. Runs the chunker to split file into variable-size chunks
   d. For each chunk: dedup → compress → encrypt → store
6. Updates job status to COMPLETED with metrics
7. Publishes `BACKUP_JOB_COMPLETED` event

**File processing pipeline (`_process_file`):**

For each chunk in each file:
1. **Chunk:** Split file at content-defined boundary (Rabin fingerprint)
2. **Hash:** Compute SHA-256 hash of chunk data
3. **Dedup:** Check if hash exists in global dedup store
   - If exists: increment reference count, skip storage
   - If new: proceed to compress
4. **Compress:** Apply zstd compression
5. **Encrypt:** Encrypt compressed data with AES-256-GCM
6. **Store:** Quorum write to 3 storage nodes
7. **Index:** Register chunk metadata, location, and file-chunk mapping

---

#### 2.2.3 Restore Engine

**File:** `src/restore/engine.py`

The Restore Engine handles point-in-time and latest-version file recovery.

**Key methods:**

| Method | Description |
|--------|-------------|
| `restore_latest(user_id, policy_id, file_path, restore_path)` | Restores the latest version of a file |
| `restore_point_in_time(user_id, policy_id, file_path, target_timestamp, restore_path)` | Restores a file as it existed at a specific timestamp |
| `_restore_file(user_id, version, file_path, restore_path)` | Walks the version chain and reconstructs a file |
| `_collect_chunk_hashes(version, file_id)` | Collects all chunk hashes from the version chain in order |
| `_retrieve_chunk(chunk_hash)` | Retrieves, decrypts, and decompresses a single chunk |

**Restore job lifecycle:**

```
PENDING → RUNNING → COMPLETED (or FAILED)
```

**Version chain traversal:**

The restore engine walks from the target version backward through the version chain:

1. Start at the target version (latest or point-in-time)
2. Collect all file chunks for the file at this version
3. For incremental versions, newer chunks override older ones at the same index
4. When a FULL version is reached, stop (it has all base chunks)
5. If parent_version_id exists and type is not FULL, continue to parent

This gives a complete ordered list of chunk hashes that represent the file at the target point in time.

**Chunk retrieval and assembly:**

For each chunk hash in order:
1. Try in-memory dedup store first (fast path)
2. Fall back to storage cluster (quorum read from any online node)
3. Decrypt with AES-256-GCM
4. Decompress with zstd
5. Append to reconstruction buffer

**Integrity verification:**

After all chunks are assembled:
```
SHA-256(reconstructed_file) == stored_file_hash
```

If mismatch: raise RuntimeError (data corruption detected)

---

#### 2.2.4 Metadata Service

**File:** `src/metadata/store.py`

The Metadata Service (simulated as `MetadataStore`) manages all relational data in memory. In production, this would be backed by PostgreSQL and Redis.

**Managed entities:**

| Entity | Fields | Purpose |
|--------|--------|---------|
| `BackupPolicy` | id, user_id, name, schedule_cron, retention_days, backup_type, source_paths, enabled | Backup schedule configuration |
| `BackupJob` | id, policy_id, user_id, backup_type, status, total_bytes, new_chunks_count, dedup_savings_bytes | Execution record for a single backup run |
| `BackupVersion` | id, job_id, policy_id, user_id, version_number, parent_version_id, backup_type, snapshot_time, status, expires_at | Immutable snapshot in version chain |
| `FileMetadata` | id, user_id, file_path, file_size, file_mod_time, file_hash | File-level metadata and integrity hash |
| `ChunkRecord` | id, hash, size_bytes, compressed_size_bytes, reference_count | Deduplicated chunk index |
| `FileChunk` | file_id, chunk_id, chunk_index, offset, size_bytes | Ordered mapping of files to chunks |
| `ChunkLocation` | chunk_id, storage_node_id, storage_path, is_primary | Physical location of each replica |
| `RestoreJob` | id, user_id, version_id, restore_path, status, files_restored, total_bytes | Restore operation tracking |

---

### 2.3 Event Bus (Pub/Sub)

**File:** `src/messaging/pubsub.py`

Pub/Sub is the central nervous system of SafeVault. All inter-component communication is asynchronous and event-driven.

#### EventTopic enum

| Value | Topic string |
|-------|-------------|
| `SCHEDULER_BACKUP_DUE` | `scheduler.backup.due` |
| `BACKUP_JOB_STARTED` | `backup.job.started` |
| `BACKUP_JOB_COMPLETED` | `backup.job.completed` |
| `BACKUP_JOB_FAILED` | `backup.job.failed` |
| `BACKUP_CHUNK_STORED` | `backup.chunk.stored` |
| `RESTORE_JOB_STARTED` | `restore.job.started` |
| `RESTORE_JOB_COMPLETED` | `restore.job.completed` |
| `RESTORE_JOB_FAILED` | `restore.job.failed` |
| `STORAGE_NODE_HEARTBEAT` | `storage.node.heartbeat` |
| `STORAGE_NODE_FAILED` | `storage.node.failed` |
| `REPLICATION_SYNC_REQUIRED` | `replication.sync.required` |
| `MONITORING_ALERT` | `monitoring.alert` |

#### Event dataclass

| Field | Type | Description |
|-------|------|-------------|
| `id` | str (UUID) | Unique event identifier |
| `topic` | str | EventTopic value |
| `event_type` | str | Sub-type (e.g., "full", "incremental", "error") |
| `payload` | dict | JSON-serializable data |
| `producer` | str | Component name that created the event |
| `timestamp` | float | Unix timestamp of creation |

#### PubSubBus API

| Method | Description |
|--------|-------------|
| `subscribe(topic, callback)` | Register a callback for a topic |
| `unsubscribe(topic, callback)` | Remove a callback subscription |
| `publish(event)` | Publish a event to all subscribers of its topic |
| `get_history(topic)` | Retrieve event history, optionally filtered by topic |
| `clear()` | Clear all subscriptions and history |

#### Topic routing table

| Topic | Producer | Consumer(s) | Payload |
|-------|----------|-------------|---------|
| `scheduler.backup.due` | BackupScheduler | BackupEngine | `{policy_id, backup_type}` |
| `backup.job.started` | BackupEngine | MetricsCollector | `{job_id, policy_id, type}` |
| `backup.job.completed` | BackupEngine | MetricsCollector | `{job_id, version_id, bytes, dedup_savings, unique_chunks}` |
| `backup.job.failed` | BackupEngine | MetricsCollector | `{job_id, error}` |
| `restore.job.started` | RestoreEngine | MetricsCollector | `{job_id, version_id, file_path}` |
| `restore.job.completed` | RestoreEngine | MetricsCollector | `{job_id, file_path, bytes}` |
| `restore.job.failed` | RestoreEngine | MetricsCollector | `{job_id, error}` |

**Why Pub/Sub?**
1. **Decoupling** — Backup engine doesn't need to know about monitoring, replication, or billing
2. **Resilience** — If monitoring is down, backup jobs still run
3. **Scalability** — Multiple workers can consume from the same topic
4. **Observability** — Every state change is an event, enabling audit trails

---

### 2.4 Core Processing Pipeline

#### 2.4.1 Chunker (Content-Defined Chunking)

**File:** `src/core/chunking.py`

| Feature | Detail |
|---------|--------|
| Algorithm | Rabin fingerprint rolling hash |
| Window size | 48 bytes |
| Min chunk | 4,096 bytes (configurable) |
| Avg chunk | 8,192 bytes |
| Max chunk | 16,384 bytes |
| Target mask | `0x00001FFF` (13 bits → avg 8KB) |

The chunker scans byte-by-byte through the file. At each position, it computes a rolling hash over the last 48 bytes. When `(hash & target_mask) == 0`, a chunk boundary is emitted. If no boundary is found before `max_chunk`, a boundary is forced.

**Key methods:**

| Method | Description |
|--------|-------------|
| `RabinChunker.__init__(min_chunk, avg_chunk, max_chunk)` | Configure chunk size parameters |
| `chunk(data)` | Split bytes into list of `(offset, size)` tuples |
| `FixedChunker.__init__(chunk_size)` | Fixed-size chunker for testing |
| `FixedChunker.chunk(data)` | Split bytes into fixed-size chunks |

---

#### 2.4.2 Dedup Engine

**File:** `src/core/dedup.py`

| Feature | Detail |
|---------|--------|
| Hashing | SHA-256 |
| Index | In-memory dictionary (hash → data, hash → ref_count) |
| Bloom filter | Simplified set-based (in production: Redis Bloom) |
| Reference counting | Incremented on store, decremented on release |
| GC trigger | reference_count = 0 after release |

**Key methods:**

| Method | Description |
|--------|-------------|
| `store(chunk_data)` | Store chunk; returns SHA-256 hash |
| `lookup(chunk_hash)` | Retrieve chunk data by hash |
| `release(chunk_hash)` | Decrement reference count |
| `ref_count(chunk_hash)` | Get current reference count |
| `might_exist(chunk_hash)` | Bloom filter check (fast negative) |
| `total_unique_chunks()` | Count of unique chunks |
| `total_storage_bytes()` | Total bytes of stored chunk data |

---

#### 2.4.3 Compressor

**File:** `src/core/compression.py`

| Feature | Detail |
|---------|--------|
| Algorithm | Zstandard (zstd) |
| Default level | 3 |
| Range | 1 (fast) to 22 (ultra) |
| Disable flag | `enable=False` for passthrough |

**Key methods:**

| Method | Description |
|--------|-------------|
| `compress(data)` | Compress bytes with zstd |
| `decompress(data)` | Decompress bytes with zstd |

---

#### 2.4.4 Encryptor

**File:** `src/core/encryption.py`

| Feature | Detail |
|---------|--------|
| Algorithm | AES-256-GCM |
| Key size | 32 bytes (256 bits) |
| IV size | 12 bytes (96 bits) |
| Tag size | 16 bytes (128 bits) |
| Key derivation | SHA-256 from password + salt |
| Disable flag | `enable=False` for passthrough |

**Key methods:**

| Method | Description |
|--------|-------------|
| `encrypt(plaintext)` | Returns `(iv + ciphertext + tag, iv)` |
| `decrypt(data, iv)` | Decrypts and returns plaintext |
| `from_password(password, salt)` | Factory method for password-derived keys |

---

### 2.5 Storage Layer

#### 2.5.1 Storage Node

**File:** `src/storage/node.py`

| Feature | Detail |
|---------|--------|
| Capacity | Configurable per node (default 1 TB) |
| Statuses | ONLINE, OFFLINE, DEGRADED, MAINTENANCE |
| Integrity | SHA-256 computed at store time, verified on every read |
| Operations | store, retrieve, delete, has_chunk |

**Key methods:**

| Method | Description |
|--------|-------------|
| `store(chunk_hash, data)` | Store data; rejects duplicates |
| `retrieve(chunk_hash)` | Read data with integrity check |
| `delete(chunk_hash)` | Remove data from node |
| `has_chunk(chunk_hash)` | Check existence |
| `set_status(status)` | Change node status (simulate failures) |

---

#### 2.5.2 Storage Cluster

**File:** `src/storage/cluster.py`

| Feature | Detail |
|---------|--------|
| Replication factor | 3 (configurable) |
| Quorum write | N/2 + 1 acknowledgments required |
| Quorum read | Read from any online node with the chunk |
| Node selection | 2 from primary region + 1 from another region |
| Rollback | Failed writes are rolled back on all successful nodes |

**Key methods:**

| Method | Description |
|--------|-------------|
| `add_node(node)` | Register a storage node |
| `remove_node(node_id)` | Remove a node from the cluster |
| `online_nodes(region)` | Get all online nodes, optionally filtered by region |
| `select_nodes_for_write(count, primary_region)` | Select nodes: 2 primary, 1 cross-region |
| `quorum_write(chunk_hash, data, region)` | Quorum-based write to N nodes |
| `quorum_read(chunk_hash)` | Read from first online node that has the chunk |

---

### 2.6 Monitoring System

**File:** `src/monitoring/metrics.py`

| Feature | Detail |
|---------|--------|
| Collection | Automatic via Pub/Sub subscription |
| Counters | `backup.completed`, `backup.failed`, `restore.completed`, `restore.failed` |
| Samples | Backup bytes, dedup savings, storage node heartbeats |
| Alerts | Critical alerts for backup/restore failures |

**Key methods:**

| Method | Description |
|--------|-------------|
| `get_counter(name)` | Get counter value by name |
| `get_alerts(severity)` | Get alerts, optionally filtered by severity |
| `summary()` | Get aggregated metrics dictionary |

---

## 3. Workflow Sequences

### 3.1 Full Backup

```
Step 1: Client creates backup policy via API Gateway
Step 2: Scheduler registers policy, computes next run time
Step 3: Scheduler.check_due() returns policy_id when time elapses
Step 4: BackupEngine.run_backup(policy_id, files) is called
Step 5: Backup type determined (FULL if no prior version exists)
Step 6: For each file:
   a. Compute SHA-256 file hash
   b. Register/update FileMetadata
   c. Run Chunker.chunk(data) → list of (offset, size)
   d. For each chunk:
      i.   Compute SHA-256 chunk hash
      ii.  Check DedupStore for existing hash
      iii. If new: Compressor.compress() → Encryptor.encrypt()
      iv.  StorageCluster.quorum_write() to 3 nodes
      v.   Register ChunkRecord + ChunkLocation
      vi.  Link FileChunk (file_id, chunk_id, index)
Step 7: Mark job COMPLETED, publish BACKUP_JOB_COMPLETED
Step 8: MetricsCollector receives event, updates counters
```

### 3.2 Incremental Backup

```
Step 1: Same as Full up to Step 4
Step 2: Backup type = INCREMENTAL (parent = last version)
Step 3: Version number incremented
Step 4: For each file:
   a. Same pipeline as Full
   b. Dedup finds existing chunks → reference count incremented
   c. Only truly new chunks are compressed/encrypted/stored
Step 5: New chunks count and dedup savings calculated
Step 6: Version linked to parent via parent_version_id
Step 7: Restore engine can reconstruct by walking chain
```

### 3.3 Latest-Version Restore

```
Step 1: Client requests restore_latest(policy_id, file_path)
Step 2: MetadataStore.get_latest_version(policy_id)
Step 3: Version chain traversal:
   a. Current version → get file chunks
   b. If INCREMENTAL → check parent
   c. Repeat until FULL version reached
Step 4: For each chunk index, use latest version's mapping
Step 5: Build ordered list of chunk hashes
Step 6: For each chunk:
   a. DedupStore.lookup() or StorageCluster.quorum_read()
   b. Encryptor.decrypt() → Compressor.decompress()
Step 7: Assemble buffer in chunk order
Step 8: Verify SHA-256(file) matches stored hash
Step 9: Return reconstructed bytes
```

### 3.4 Point-in-Time Restore

```
Step 1: Client requests restore_point_in_time(policy_id, file_path, timestamp)
Step 2: Filter versions: policy_id matches AND snapshot_time <= timestamp
Step 3: Select latest version from filtered set
Step 4: Same version chain traversal as latest restore from Step 3
```

---

## 4. Fault Tolerance Design

| Failure Scenario | Mitigation | Implementation |
|-----------------|------------|----------------|
| Storage node fails | Replication (3×); quorum read from healthy nodes | `StorageNode.set_status(OFFLINE)` → `quorum_read` skips offline nodes |
| Chunk checksum mismatch | SHA-256 computed at store, verified on every read | `StorageNode.retrieve()` compares `_compute_hash(data) != stored_hash` |
| Backup job interrupted | No explicit checkpoint needed for in-memory sim | Production: checkpoint file written after each file |
| Quorum write fails | Rollback all successful writes | `quorum_write()` catches exceptions, calls `node.delete()` on successes |
| Insufficient nodes | Error raised before write | `select_nodes_for_write()` checks `len(candidates) >= count` |
| Corrupted encryption key | `from_password` with salt for deterministic keys | Key never stored in code |
| AEAD tampering | AES-GCM authenticates ciphertext | `decrypt()` raises on tag mismatch |

---

## 5. Scalability Design

| Dimension | Strategy |
|-----------|----------|
| Storage capacity | Add nodes horizontally (each node = 1 TB default) |
| Backup throughput | Process files sequentially (production: parallel workers) |
| Dedup index | In-memory dictionary (production: Redis Cluster sharded by hash prefix) |
| Metadata | In-memory dicts (production: PostgreSQL read replicas + sharding) |
| Scheduling | Single-threaded loop (production: distributed Quartz Scheduler) |
| Monitoring | Pub/Sub subscription scales with event volume |

---

## 6. Security Architecture

| Concern | Implementation |
|---------|---------------|
| Data confidentiality | AES-256-GCM per-chunk encryption |
| Data integrity | SHA-256 file hash, SHA-256 per-chunk stored hash |
| Authentication | Simulated via user_id in metadata (production: OAuth2/IAM) |
| Authorization | Policy owned by user_id (production: RBAC) |
| Key management | Random key per Encryptor instance (production: KMS) |
| Tamper detection | GCM authentication tag on every chunk |
| Version immutability | Append-only version chain (no deletes, only status changes) |

---

## 7. Real-World References

| Feature | Reference System |
|---------|-----------------|
| Content-Defined Chunking (Rabin) | BorgBackup, restic, Data Domain |
| Global Deduplication (SHA-256) | Dell EMC Data Domain, ZFS |
| Zstandard Compression | Facebook Zstandard (used in AWS, Google) |
| AES-256-GCM Encryption | Standard in cloud KMS (AWS, GCP, Azure) |
| Pub/Sub Event Architecture | Apache Kafka (Uber's backup, Netflix Chaos Monkey) |
| Version Chain / Snapshots | AWS EBS Snapshots, Veeam Backup |
| Quorum Replication (3×) | Apache BookKeeper, Ceph, Cassandra |
| Quorum Write / Read | Apache Cassandra (R + W > N) |
| Bloom Filter Dedup | Cassandra, HBase, Redis |
| Incremental Backup Chain | Borg, restic, Duplicati |
| Content-Defined Dedup | Borg (BorgChunk), restic |
