# SafeVault — Database Schema Design

## Design Rationale

SafeVault uses a dual-database strategy:

| Database             | Role                         | Justification                                                                                                                                                             |
| -------------------- | ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **PostgreSQL** | Prim, si iary metadata store | ACID compliance, strong consistency (critical for backup metadata), JSONB for flexible policy config, row-level security for multi-tenancy, read replicas for scalability |
| **Redis**      | Cache + Queue + Dedup        | Sub-millisecond lookups for dedup Bloom filter, distributed locks for concurrency, job queues for async processing, TTL-based cache eviction                              |

## Entity-Relationship Diagram (Text)

```
users ──1:N── backup_policies ──1:N── backup_jobs ──1:1── backup_versions
                                                             │
users ──1:N── file_metadata ──1:N── version_files ──N:1── backup_versions
                                        │
file_metadata ──1:N── file_chunks ──N:1── chunks
                                              │
chunks ──1:N── chunk_locations ──N:1── storage_nodes
                                              │
users ──1:N── restore_jobs ──N:1── backup_versions
```

---

## PostgreSQL Table Definitions

### 1. `users`

User accounts and storage quota management.

| Column                  | Type                                | Constraints                   | Description                        |
| ----------------------- | ----------------------------------- | ----------------------------- | ---------------------------------- |
| `id`                  | `UUID`                            | PK, DEFAULT gen_random_uuid() | Unique user identifier             |
| `name`                | `VARCHAR(255)`                    | NOT NULL                      | Display name                       |
| `email`               | `VARCHAR(255)`                    | UNIQUE, NOT NULL              | Login email                        |
| `plan`                | `ENUM('free','pro','enterprise')` | NOT NULL, DEFAULT 'free'      | Service tier                       |
| `storage_quota_bytes` | `BIGINT`                          | NOT NULL, DEFAULT 1073741824  | Max allowed storage (1 GB default) |
| `storage_used_bytes`  | `BIGINT`                          | NOT NULL, DEFAULT 0           | Current storage used               |
| `created_at`          | `TIMESTAMPTZ`                     | NOT NULL, DEFAULT NOW()       |                                    |
| `updated_at`          | `TIMESTAMPTZ`                     | NOT NULL, DEFAULT NOW()       |                                    |
| `deleted_at`          | `TIMESTAMPTZ`                     | NULLABLE                      | Soft-delete timestamp              |

**Indexes:** `idx_users_email ON users(email)` (unique)

---

### 2. `backup_policies`

Backup schedule configurations.

| Column             | Type                           | Constraints               | Description                          |
| ------------------ | ------------------------------ | ------------------------- | ------------------------------------ |
| `id`             | `UUID`                       | PK                        |                                      |
| `user_id`        | `UUID`                       | FK → users(id), NOT NULL | Owner                                |
| `name`           | `VARCHAR(255)`               | NOT NULL                  | e.g., "Daily DB Backup"              |
| `schedule_cron`  | `VARCHAR(100)`               | NOT NULL                  | Cron expression (e.g.,`0 2 * * *`) |
| `retention_days` | `INT`                        | NOT NULL, DEFAULT 30      | Days to keep versions                |
| `type`           | `ENUM('full','incremental')` | NOT NULL                  | Default backup type                  |
| `source_paths`   | `TEXT[]`                     | NOT NULL                  | List of file paths/globs to back up  |
| `enabled`        | `BOOLEAN`                    | NOT NULL, DEFAULT true    | Whether schedule is active           |
| `created_at`     | `TIMESTAMPTZ`                | NOT NULL, DEFAULT NOW()   |                                      |

**Indexes:**

- `idx_policies_user ON backup_policies(user_id)`
- `idx_policies_enabled ON backup_policies(enabled) WHERE enabled = true`

---

### 3. `backup_jobs`

Execution records for each backup run.

| Column                  | Type                                                           | Constraints                         | Description                       |
| ----------------------- | -------------------------------------------------------------- | ----------------------------------- | --------------------------------- |
| `id`                  | `UUID`                                                       | PK                                  |                                   |
| `policy_id`           | `UUID`                                                       | FK → backup_policies(id), NOT NULL | Parent policy                     |
| `user_id`             | `UUID`                                                       | FK → users(id), NOT NULL           | Owner                             |
| `type`                | `ENUM('full','incremental')`                                 | NOT NULL                            | Type of this run                  |
| `status`              | `ENUM('pending','running','completed','failed','cancelled')` | NOT NULL, DEFAULT 'pending'         | Current status                    |
| `started_at`          | `TIMESTAMPTZ`                                                | NULLABLE                            | When execution started            |
| `completed_at`        | `TIMESTAMPTZ`                                                | NULLABLE                            | When execution ended              |
| `total_bytes`         | `BIGINT`                                                     | NOT NULL, DEFAULT 0                 | Total data processed              |
| `new_chunks_count`    | `INT`                                                        | NOT NULL, DEFAULT 0                 | Unique chunks stored              |
| `dedup_savings_bytes` | `BIGINT`                                                     | NOT NULL, DEFAULT 0                 | Bytes saved via dedup             |
| `error_message`       | `TEXT`                                                       | NULLABLE                            | Failure details                   |
| `checkpoint_path`     | `TEXT`                                                       | NULLABLE                            | Resume point for interrupted jobs |
| `created_at`          | `TIMESTAMPTZ`                                                | NOT NULL, DEFAULT NOW()             |                                   |

**Indexes:**

- `idx_jobs_policy ON backup_jobs(policy_id)`
- `idx_jobs_user ON backup_jobs(user_id)`
- `idx_jobs_status ON backup_jobs(status)`

---

### 4. `backup_versions`

Immutable version records forming the backup chain.

| Column                | Type                                   | Constraints                         | Description                              |
| --------------------- | -------------------------------------- | ----------------------------------- | ---------------------------------------- |
| `id`                | `UUID`                               | PK                                  |                                          |
| `job_id`            | `UUID`                               | FK → backup_jobs(id), NOT NULL     | Source job                               |
| `policy_id`         | `UUID`                               | FK → backup_policies(id), NOT NULL | Parent policy                            |
| `user_id`           | `UUID`                               | FK → users(id), NOT NULL           | Owner                                    |
| `version_number`    | `INT`                                | NOT NULL                            | Monotonically increasing per policy      |
| `parent_version_id` | `UUID`                               | FK → backup_versions(id), NULLABLE | Previous version (for incremental chain) |
| `type`              | `ENUM('full','incremental')`         | NOT NULL                            | Snapshot type                            |
| `snapshot_time`     | `TIMESTAMPTZ`                        | NOT NULL                            | Point-in-time of this snapshot           |
| `manifest_hash`     | `VARCHAR(64)`                        | NULLABLE                            | SHA-256 of version manifest              |
| `status`            | `ENUM('active','expired','deleted')` | NOT NULL, DEFAULT 'active'          | Lifecycle status                         |
| `expires_at`        | `TIMESTAMPTZ`                        | NULLABLE                            | Auto-expiry based on retention           |
| `created_at`        | `TIMESTAMPTZ`                        | NOT NULL, DEFAULT NOW()             |                                          |

**Indexes:**

- `idx_versions_policy ON backup_versions(policy_id, version_number)`
- `idx_versions_parent ON backup_versions(parent_version_id)`
- `idx_versions_status ON backup_versions(status)`
- `idx_versions_expiry ON backup_versions(expires_at) WHERE status = 'active'`

**Constraints:**

- `UNIQUE(policy_id, version_number)` — no duplicate version numbers per policy
- `CHECK(version_number > 0)`

---

### 5. `file_metadata`

File-level metadata and integrity information.

| Column            | Type             | Constraints               | Description                      |
| ----------------- | ---------------- | ------------------------- | -------------------------------- |
| `id`            | `UUID`         | PK                        |                                  |
| `user_id`       | `UUID`         | FK → users(id), NOT NULL | Owner                            |
| `file_path`     | `TEXT`         | NOT NULL                  | Absolute path in backup          |
| `file_size`     | `BIGINT`       | NOT NULL                  |                                  |
| `file_mod_time` | `TIMESTAMPTZ`  | NOT NULL                  | mtime at backup time             |
| `file_hash`     | `VARCHAR(64)`  | NOT NULL                  | SHA-256 of full file (integrity) |
| `mime_type`     | `VARCHAR(255)` | NULLABLE                  |                                  |
| `created_at`    | `TIMESTAMPTZ`  | NOT NULL, DEFAULT NOW()   |                                  |

**Indexes:**

- `idx_files_user_path ON file_metadata(user_id, file_path)` (unique)
- `idx_files_hash ON file_metadata(file_hash)`

---

### 6. `version_files`

Links backup versions to the files they contain.

| Column               | Type       | Constraints               | Description                    |
| -------------------- | ---------- | ------------------------- | ------------------------------ |
| `id`               | `UUID`   | PK                        |                                |
| `version_id`       | `UUID`   | FK → backup_versions(id) |                                |
| `file_id`          | `UUID`   | FK → file_metadata(id)   |                                |
| `chunk_count`      | `INT`    | NOT NULL                  | Number of chunks for this file |
| `total_size_bytes` | `BIGINT` | NOT NULL                  |                                |

**Indexes:** `idx_version_files_version ON version_files(version_id)`

---

### 7. `chunks`

Deduplicated chunk index.

| Column                    | Type             | Constraints              | Description                            |
| ------------------------- | ---------------- | ------------------------ | -------------------------------------- |
| `id`                    | `UUID`         | PK                       |                                        |
| `hash`                  | `VARCHAR(64)`  | UNIQUE, NOT NULL         | SHA-256 of chunk content               |
| `size_bytes`            | `INT`          | NOT NULL                 | Uncompressed size                      |
| `compressed_size_bytes` | `INT`          | NOT NULL                 | After zstd compression                 |
| `compression_algorithm` | `VARCHAR(20)`  | NOT NULL, DEFAULT 'zstd' |                                        |
| `encryption_key_id`     | `VARCHAR(255)` | NULLABLE                 | KMS key reference                      |
| `reference_count`       | `INT`          | NOT NULL, DEFAULT 1      | Number of files referencing this chunk |
| `created_at`            | `TIMESTAMPTZ`  | NOT NULL, DEFAULT NOW()  |                                        |

**Indexes:** `idx_chunks_hash ON chunks(hash)` (unique)

---

### 8. `file_chunks`

Ordered mapping of files to their constituent chunks.

| Column          | Type       | Constraints                       | Description                  |
| --------------- | ---------- | --------------------------------- | ---------------------------- |
| `id`          | `UUID`   | PK                                |                              |
| `file_id`     | `UUID`   | FK → file_metadata(id), NOT NULL |                              |
| `chunk_id`    | `UUID`   | FK → chunks(id), NOT NULL        |                              |
| `chunk_index` | `INT`    | NOT NULL                          | Order in file (0-based)      |
| `offset`      | `BIGINT` | NOT NULL                          | Byte offset in original file |
| `size_bytes`  | `INT`    | NOT NULL                          |                              |

**Indexes:**

- `idx_file_chunks_file ON file_chunks(file_id, chunk_index)`
- `idx_file_chunks_chunk ON file_chunks(chunk_id)`

**Constraints:** `UNIQUE(file_id, chunk_index)` — one chunk per index per file

---

### 9. `chunk_locations`

Physical location of each chunk replica across storage nodes.

| Column              | Type            | Constraints                       | Description              |
| ------------------- | --------------- | --------------------------------- | ------------------------ |
| `id`              | `UUID`        | PK                                |                          |
| `chunk_id`        | `UUID`        | FK → chunks(id), NOT NULL        |                          |
| `storage_node_id` | `UUID`        | FK → storage_nodes(id), NOT NULL |                          |
| `storage_path`    | `TEXT`        | NOT NULL                          | Path on the storage node |
| `is_primary`      | `BOOLEAN`     | NOT NULL, DEFAULT false           | Primary replica flag     |
| `created_at`      | `TIMESTAMPTZ` | NOT NULL, DEFAULT NOW()           |                          |

**Indexes:** `idx_chunk_locations_chunk ON chunk_locations(chunk_id)`

---

### 10. `storage_nodes`

Cluster inventory of storage nodes.

| Column             | Type                                                  | Constraints                | Description            |
| ------------------ | ----------------------------------------------------- | -------------------------- | ---------------------- |
| `id`             | `UUID`                                              | PK                         |                        |
| `name`           | `VARCHAR(255)`                                      | NOT NULL                   | Human-readable name    |
| `region`         | `VARCHAR(100)`                                      | NOT NULL                   | e.g.,`us-east-1`     |
| `zone`           | `VARCHAR(100)`                                      | NOT NULL                   | e.g.,`us-east-1a`    |
| `endpoint`       | `TEXT`                                              | NOT NULL                   | gRPC/REST endpoint URL |
| `capacity_bytes` | `BIGINT`                                            | NOT NULL                   | Maximum capacity       |
| `used_bytes`     | `BIGINT`                                            | NOT NULL, DEFAULT 0        | Currently used         |
| `status`         | `ENUM('online','offline','degraded','maintenance')` | NOT NULL, DEFAULT 'online' |                        |
| `last_heartbeat` | `TIMESTAMPTZ`                                       | NULLABLE                   |                        |
| `created_at`     | `TIMESTAMPTZ`                                       | NOT NULL, DEFAULT NOW()    |                        |

**Indexes:** `idx_nodes_region ON storage_nodes(region, status)`

---

### 11. `restore_jobs`

Tracking for restore operations.

| Column             | Type                                                           | Constraints                         | Description      |
| ------------------ | -------------------------------------------------------------- | ----------------------------------- | ---------------- |
| `id`             | `UUID`                                                       | PK                                  |                  |
| `user_id`        | `UUID`                                                       | FK → users(id), NOT NULL           |                  |
| `version_id`     | `UUID`                                                       | FK → backup_versions(id), NOT NULL | Source version   |
| `restore_path`   | `TEXT`                                                       | NOT NULL                            | Destination path |
| `status`         | `ENUM('pending','running','completed','failed','cancelled')` | NOT NULL, DEFAULT 'pending'         |                  |
| `files_restored` | `INT`                                                        | NOT NULL, DEFAULT 0                 |                  |
| `total_bytes`    | `BIGINT`                                                     | NOT NULL, DEFAULT 0                 |                  |
| `started_at`     | `TIMESTAMPTZ`                                                | NULLABLE                            |                  |
| `completed_at`   | `TIMESTAMPTZ`                                                | NULLABLE                            |                  |
| `error_message`  | `TEXT`                                                       | NULLABLE                            |                  |
| `created_at`     | `TIMESTAMPTZ`                                                | NOT NULL, DEFAULT NOW()             |                  |

**Indexes:** `idx_restore_user ON restore_jobs(user_id, status)`

---

### 12. `events`

Immutable audit log of all Pub/Sub events.

| Column         | Type             | Constraints             | Description                      |
| -------------- | ---------------- | ----------------------- | -------------------------------- |
| `id`         | `UUID`         | PK                      |                                  |
| `topic`      | `VARCHAR(255)` | NOT NULL                | Pub/Sub topic                    |
| `event_type` | `VARCHAR(255)` | NOT NULL                | Event sub-type                   |
| `payload`    | `JSONB`        | NOT NULL                | Full event payload               |
| `producer`   | `VARCHAR(255)` | NOT NULL                | Component that emitted the event |
| `created_at` | `TIMESTAMPTZ`  | NOT NULL, DEFAULT NOW() |                                  |

**Indexes:**

- `idx_events_topic ON events(topic, created_at)`
- `idx_events_producer ON events(producer, created_at)`

---

## Redis Data Structures

| Key Pattern                         | Type         | TTL   | Purpose                                                            |
| ----------------------------------- | ------------ | ----- | ------------------------------------------------------------------ |
| `dedup:bloom:{hash_prefix}`       | Bloom Filter | ∞    | Fast negative dedup check (false positives but no false negatives) |
| `dedup:hash:{sha256}`             | String       | 3600s | Cached chunk location for hot chunks                               |
| `lock:backup:{job_id}`            | String (NX)  | 300s  | Distributed mutex for backup job concurrency                       |
| `lock:restore:{job_id}`           | String (NX)  | 300s  | Distributed mutex for restore job concurrency                      |
| `queue:backup:pending`            | List         | —    | Pending backup job IDs (LPUSH/RPOP)                                |
| `queue:restore:pending`           | List         | —    | Pending restore job IDs (LPUSH/RPOP)                               |
| `schedule:next:{policy_id}`       | Sorted Set   | —    | Next scheduled run time (score = timestamp)                        |
| `session:{token}`                 | String       | 1800s | API session token → user_id mapping                               |
| `rate_limit:{user_id}:{endpoint}` | Counter      | 1s    | Sliding window rate limit counter                                  |

---

## Sample Queries

### Find all active versions for a user's policy

```sql
SELECT v.version_number, v.type, v.snapshot_time, v.status
FROM backup_versions v
JOIN backup_policies p ON v.policy_id = p.id
WHERE p.user_id = 'user-uuid'
  AND v.status = 'active'
ORDER BY v.version_number DESC;
```

### Get chunk counts for a specific file version

```sql
SELECT f.file_path, COUNT(fc.id) as chunk_count, SUM(c.size_bytes) as total_bytes
FROM version_files vf
JOIN file_metadata f ON vf.file_id = f.id
LEFT JOIN file_chunks fc ON f.id = fc.file_id
LEFT JOIN chunks c ON fc.chunk_id = c.id
WHERE vf.version_id = 'version-uuid'
GROUP BY f.file_path;
```

### Find under-replicated chunks

```sql
SELECT c.hash, COUNT(cl.id) as replica_count
FROM chunks c
LEFT JOIN chunk_locations cl ON c.id = cl.chunk_id
GROUP BY c.hash
HAVING COUNT(cl.id) < 3;
```

### Calculate dedup savings for a job

```sql
SELECT
  j.id,
  j.total_bytes,
  j.new_chunks_count,
  j.dedup_savings_bytes,
  ROUND(j.dedup_savings_bytes * 100.0 / NULLIF(j.total_bytes, 0), 1) as savings_pct
FROM backup_jobs j
WHERE j.id = 'job-uuid';
```

### Get expired versions for cleanup

```sql
SELECT v.id, v.version_number, v.policy_id
FROM backup_versions v
WHERE v.status = 'active'
  AND v.expires_at IS NOT NULL
  AND v.expires_at < NOW();
```

---

## Migration Strategy

For production deployment, schema migrations would be managed via Alembic:

```bash
# Generate a new migration
alembic revision --autogenerate -m "add_chunk_locations_table"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1
```

Initial migration files would be in a `migrations/` directory with versioned SQL scripts for each table creation and index building.
