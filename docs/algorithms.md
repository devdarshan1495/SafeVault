# SafeVault — Algorithmic Design

---

## 1. Content-Defined Chunking (Rabin Fingerprint)

### Purpose

Split files into variable-size chunks based on content, not fixed offsets. This ensures that inserting or deleting bytes in the middle of a file only changes local chunk boundaries — maximizing deduplication across versions.

### Algorithm: Rabin Fingerprint CDC

```
Constants:
  WINDOW_SIZE  = 48 bytes
  MODULUS      = 2^31 - 1 (a large prime)
  TARGET_MASK  = 0x00001FFF  (13 bits → avg chunk = 8,192 bytes)
  MIN_CHUNK    = 4,096 bytes
  MAX_CHUNK    = 16,384 bytes

Procedure CDC_Rabin(data):
  boundaries = []
  last = 0
  hash = 0

  for i = 0 to len(data)-1:
    // Rolling hash: slide window by one byte
    if i < WINDOW_SIZE:
      hash = ((hash << 1) + data[i]) % MODULUS
    else:
      out_byte = data[i - WINDOW_SIZE]
      in_byte  = data[i]
      hash = ((hash << 1) - (out_byte << WINDOW_SIZE) + in_byte) % MODULUS

    chunk_len = i - last + 1

    // Check for boundary (content-defined)
    if chunk_len >= MIN_CHUNK:
      if (hash & TARGET_MASK) == 0:
        boundaries.push(i + 1)
        last = i + 1
        continue

    // Force boundary at max chunk
    if chunk_len >= MAX_CHUNK:
      boundaries.push(i + 1)
      last = i + 1

  // Last chunk
  if last < len(data):
    boundaries.push(len(data))

  return list of (offset, size) from boundaries
```

### Why Rabin Fingerprint?

| Property | Benefit |
|----------|---------|
| **Rolling** | O(1) per byte; no need to rehash entire buffer |
| **Content-defined** | Same content = same boundaries (even at different offsets) |
| **Deterministic** | Same file always produces same chunk boundaries |
| **Locality** | Insert/delete only affects chunks at the modification point |

### Complexity

- **Time:** O(n) — single pass over each file
- **Space:** O(1) — constant window buffer

### Implementation Reference

**File:** `src/core/chunking.py`

| Component | Line |
|-----------|------|
| Constants | 6–12 |
| Precomputed table (optimization) | `_build_table()` |
| Rolling hash computation | `chunk()` loop at line 60 |
| Min/avg/max chunk enforcement | Lines 72–80 |
| Boundary emission | Lines 73–74 |

---

## 2. Global Deduplication (SHA-256)

### Purpose

Eliminate redundant storage by identifying identical chunks through cryptographic hashing.

### Algorithm: DedupStore

```
Procedure store(chunk_data):
  hash = SHA-256(chunk_data)

  // Bloom filter: fast rejection (no false negatives)
  if bloom.might_contain(hash):
    if hash exists in main index:
      ref_count[hash] += 1
      return hash   // Duplicate — no storage needed

  // New unique chunk
  main_index[hash] = chunk_data
  ref_count[hash] = 1
  bloom.add(hash)

  return hash

Procedure release(hash):
  if hash in ref_count:
    ref_count[hash] -= 1
    if ref_count[hash] <= 0:
      delete main_index[hash]
      delete ref_count[hash]
      bloom.remove(hash)  // Rebuild periodically in production
```

### Bloom Filter Optimization

The bloom filter provides **O(1)** negative lookups. If the bloom says a hash does NOT exist, we can skip the full hash index lookup. False positives are possible but handled by the full index check.

### Complexity

- **Store:** O(1) amortized (hash computation O(chunk_size), index ops O(1))
- **Lookup:** O(1) — direct dictionary access
- **Release:** O(1)

### Implementation Reference

**File:** `src/core/dedup.py`

| Component | Line |
|-----------|------|
| SHA-256 hashing | `_hash()` at line 16 |
| Store with dedup check | `store()` at line 24 |
| Reference counting | Lines 28, 33 |
| Bloom filter (set-based) | `_bloom` at line 13 |

---

## 3. Incremental Backup

### Purpose

Only back up data that has changed since the last backup, drastically reducing backup time, storage, and network bandwidth.

### Algorithm: IncrementalBackup

```
Procedure run_backup(policy_id, source_files):
  policy = get_policy(policy_id)
  last_version = get_latest_version(policy_id)

  // Determine backup type
  if last_version is None:
    backup_type = FULL
    parent = None
  else:
    backup_type = INCREMENTAL
    parent = last_version.id

  // Create job and version records
  job = create_job(policy_id, user_id, backup_type)
  version = create_version(
    job_id = job.id,
    version_number = last_version.number + 1,
    parent_version_id = parent,
    backup_type = backup_type,
  )

  // Process each file
  for (file_path, content) in source_files:
    process_file(file_path, content, policy, version, job)

  // Report metrics
  job.total_bytes = sum of all file sizes
  job.new_chunks_count = dedup.total_unique_chunks()
  job.dedup_savings_bytes = max(0, total_bytes - dedup_stored_bytes)

  mark_completed(job, version)

Procedure process_file(file_path, content, policy, version, job):
  // File-level metadata
  file_hash = SHA-256(content)
  fm = register_file(user_id, file_path, len(content), mtime, file_hash)

  // Chunk the file
  chunks = chunker.chunk(content)

  // Process each chunk through the pipeline
  for each (offset, size) in chunks:
    chunk_data = content[offset : offset + size]

    // Step 1: Hash + Dedup
    chunk_hash = dedup.store(chunk_data)

    // Step 2: Compress (only for new chunks)
    compressed = compressor.compress(chunk_data)

    // Step 3: Encrypt
    encrypted = encryptor.encrypt(compressed)

    // Step 4: Store (quorum write to 3 nodes)
    locations = storage.quorum_write(chunk_hash, encrypted)

    // Step 5: Index metadata
    register_chunk(chunk_hash, size, compressed_size)
    add_chunk_location(chunk_record.id, node_id, path)
    link_file_chunk(fm.id, chunk_record.id, index, offset, size)
```

### Version Chain Structure

```
Version 1 (FULL)  ←── Version 2 (INCREMENTAL)  ←── Version 3 (INCREMENTAL)
   |                         |                             |
   ├── Chunk A               ├── Chunk A (ref)             ├── Chunk A (ref)
   ├── Chunk B               ├── Chunk C (NEW)             ├── Chunk C (ref)
   ├── Chunk D               └── Chunk D (ref)             ├── Chunk E (NEW)
   └── Chunk F                                             └── Chunk F (ref)

Key:
  (ref)  = referenced from previous version (no new storage)
  (NEW)  = new chunk stored
```

### Complexity

- **Time:** O(total_bytes + n_chunks × compression_cost) per backup
- **Space:** O(n_chunks) for metadata; O(unique_chunks) for storage
- **Dedup savings:** Proportional to data change rate between versions

### Implementation Reference

**File:** `src/backup/engine.py`

| Component | Line |
|-----------|------|
| Backup type determination | Lines 56–62 |
| Version creation | Lines 83–91 |
| File processing loop | Lines 98–103 |
| Full dedup pipeline | `_process_file()` at line 145 |
| Chunk iteration | Lines 165–195 |

---

## 4. Snapshot-Based Recovery (Point-in-Time Restore)

### Purpose

Reconstruct a file as it existed at any point in time by walking the version chain.

### Algorithm: PointInTimeRestore

```
Procedure restore_latest(user_id, policy_id, file_path):
  version = get_latest_version(policy_id)
  return restore_file(user_id, version, file_path)

Procedure restore_point_in_time(user_id, policy_id, file_path, timestamp):
  // Find the latest version at or before the target timestamp
  versions = filter by: policy_id == policy_id
                        AND snapshot_time <= timestamp
                        AND status == 'active'
  if versions is empty: raise "No version at timestamp"
  version = max(versions, key = snapshot_time)
  return restore_file(user_id, version, file_path)

Procedure restore_file(user_id, version, file_path):
  job = create_restore_job(user_id, version.id, restore_path)

  // Step 1: Gather chunk hashes from version chain
  file_meta = get_file_by_path(file_path)
  chunk_hashes = collect_chunk_hashes(version, file_meta.id)

  // Step 2: Retrieve, decrypt, decompress each chunk
  reconstructed = bytearray()
  for hash in chunk_hashes:
    chunk_data = retrieve_chunk(hash)
    reconstructed.extend(chunk_data)

  // Step 3: Integrity check
  assert SHA-256(reconstructed) == file_meta.file_hash

  mark_restore_completed(job, file_path, len(reconstructed))
  return bytes(reconstructed)

Procedure collect_chunk_hashes(version, file_id):
  // Walk version chain from newest to oldest
  ordered_chunks = {}    // index → FileChunk
  current = version

  while current is not None:
    file_chunks = get_file_chunks_for_version(current.id, file_id)

    for fc in file_chunks:
      // Latest version's chunks take priority
      if fc.chunk_index not in ordered_chunks:
        ordered_chunks[fc.chunk_index] = fc
      elif current.backup_type == INCREMENTAL:
        ordered_chunks[fc.chunk_index] = fc  // Override

    if current.backup_type == FULL:
      break
    current = get_parent_version(current.parent_version_id)

  // Map chunk UUIDs to SHA-256 hashes
  sorted_indices = sorted(ordered_chunks.keys())
  return [chunk_id_to_hash[ordered_chunks[i].chunk_id] for i in sorted_indices]

Procedure retrieve_chunk(hash):
  // Fast path: in-memory dedup store
  data = dedup.lookup(hash)
  if data: return data

  // Slow path: storage cluster
  encrypted = storage.quorum_read(hash)
  compressed = decrypt(encrypted)
  return decompress(compressed)
```

### Integrity Verification

Every restored file is verified against its stored SHA-256 hash. Any corruption (chunk missing, tampered ciphertext, decompression error) raises a `RuntimeError`.

### Complexity

- **Time:** O(n_chunks × (read_cost + decrypt_cost + decompress_cost))
- **Space:** O(file_size) — reconstructs entire file in memory

### Implementation Reference

**File:** `src/restore/engine.py`

| Component | Line |
|-----------|------|
| Latest restore | `restore_latest()` at line 37 |
| Point-in-time restore | `restore_point_in_time()` at line 47 |
| Version chain traversal | `_collect_chunk_hashes()` at line 137 |
| Chunk retrieval | `_retrieve_chunk()` at line 176 |
| Integrity check | Lines 100–103 |

---

## 5. Quorum-Based Replication

### Purpose

Ensure data durability across storage nodes with configurable replication factor and quorum consistency.

### Algorithm: QuorumWrite

```
Constants:
  N = replication_factor (default: 3)
  W = write_quorum = floor(N/2) + 1 (default: 2)

Procedure quorum_write(chunk_hash, data, region = "us-east-1"):
  // Select 2 nodes from primary region + 1 from cross-region
  primary_nodes = online_nodes(region)
  other_nodes   = online_nodes(excluding region)

  selected = sample(primary_nodes, 2)
  if len(selected) < N:
    selected += sample(other_nodes, 1)

  // Write to all selected nodes in parallel
  ack_count = 0
  locations = []

  for node in selected:
    try:
      node.store(chunk_hash, data)
      ack_count += 1
      locations.append((node.id, path))
    except:
      log "Write to {node.name} failed"

  // Check quorum
  if ack_count < W:
    // Rollback: delete from all successful nodes
    for (node_id, path) in locations:
      rollback(node_id, path)
    raise "Quorum write failed: {ack_count}/{N} acked"

  return locations

Procedure quorum_read(chunk_hash):
  // Try each online node until one succeeds
  for node in online_nodes():
    try:
      data = node.retrieve(chunk_hash)
      if data is not None:
        return data
    except:
      continue  // Try next node
  return None  // Chunk not found on any node
```

### Why Quorum?

| Parameter | Value | Benefit |
|-----------|-------|---------|
| N = 3 | 3 replicas | Survives 2 node failures in worst case |
| W = 2 | Write quorum | One node can fail during write without blocking |
| R = 1 | Read quorum | Fast reads (any node can serve) |
| W + R > N | 2 + 1 > 3 ✓ | Strong consistency: every read sees latest write |

### Complexity

- **Write:** O(N × chunk_size) — N parallel store operations
- **Read:** O(chunk_size) — single node read

### Implementation Reference

**File:** `src/storage/cluster.py`

| Component | Line |
|-----------|------|
| Node selection (2 primary + 1 cross-region) | `select_nodes_for_write()` at line 31 |
| Quorum write with rollback | `quorum_write()` at line 53 |
| Quorum read (first available) | `quorum_read()` at line 83 |

---

## 6. Fault Tolerance & Retry

### Exponential Backoff with Jitter

```
Procedure retry_with_backoff(operation, max_retries = 5, base_delay_s = 1):
  for attempt = 0 to max_retries - 1:
    try:
      return operation()
    except TransientError:
      delay = base_delay_s * (2 ^ attempt)  // Exponential: 1, 2, 4, 8, 16s
      delay = min(delay, 60)                // Cap at 60 seconds
      jitter = random(0, delay * 0.5)       // ±50% jitter
      sleep(delay + jitter)

  // All retries exhausted
  publish_alert(severity = "critical", error = last_exception)
  raise
```

### Node Failure Handling

```
When storage node becomes OFFLINE:
  1. Heartbeat monitor detects absence (timeout: 30s)
  2. Monitoring publishes STORAGE_NODE_FAILED event
  3. Replication Manager marks node for re-replication
  4. All chunks on failed node are re-replicated to healthy nodes
  5. On restore, quorum_read skips offline nodes automatically
```

### Checkpoint-Based Resume

For interrupted backup jobs (production feature, simulated in checkpoint_path):

```
Procedure run_backup_with_checkpoints():
  // Load checkpoint
  checkpoint = load_checkpoint(job_id)
  if checkpoint:
    remaining_files = get_files_after(checkpoint.last_file)
    log "Resuming from file {checkpoint.last_file}"
  else:
    remaining_files = get_all_files()

  // Process remaining files
  for file in remaining_files:
    process_file(file)
    save_checkpoint(job_id, file.path)
```

### Implementation Reference

- **Retry logic:** Not in simulation (production would wrap all storage operations)
- **Node failure:** `StorageNode.set_status(OFFLINE)` (`src/storage/node.py:91`)
- **Quorum read skip:** `quorum_read()` iterates only `online_nodes()` (`src/storage/cluster.py:85`)
- **Checkpoint field:** `BackupJob.checkpoint_path` (`src/metadata/store.py:60`)

---

## 7. Garbage Collection

### Purpose

Reclaim storage from chunks that are no longer referenced by any active version.

### Algorithm

```
Procedure garbage_collect(dry_run = True):
  // Find chunks with zero references, created more than 7 days ago
  candidates = SELECT * FROM chunks
               WHERE reference_count = 0
               AND created_at < NOW() - INTERVAL '7 days'

  for chunk in candidates:
    locations = get_chunk_locations(chunk.id)

    // Delete from all storage nodes
    for location in locations:
      if not dry_run:
        storage_node(location.node_id).delete(location.path)
        delete_chunk_location(location.id)

    // Remove from dedup index
    if not dry_run:
      bloom_filter.remove(chunk.hash)
      delete_chunk(chunk.id)
      log "GC'd chunk {chunk.hash} ({chunk.size_bytes} bytes)"

  return count of GC'd chunks
```

### Safety

- 7-day grace period prevents premature deletion of chunks that may be referenced by in-flight operations
- Dry-run mode for preview before actual deletion

### Implementation Reference

Chunks are never explicitly deleted in the simulation (no GC trigger). The `release()` method in `InMemoryDedupStore` (`src/core/dedup.py:40`) decrements reference counts, and chunks with zero ref count remain in memory until process exit. In production, a periodic GC job would handle physical deletion.

---

## 8. Replication Sync

### Purpose

Maintain data consistency across storage nodes, especially after node recovery.

### Algorithm

```
Procedure sync_replicas(chunk_hash):
  // Find the primary replica location
  primary_loc = SELECT * FROM chunk_locations
                WHERE chunk_hash = chunk_hash AND is_primary = true

  // Get all other locations that should have this chunk
  expected_nodes = get_nodes_for_hash(chunk_hash)  // by consistent hashing

  for node in expected_nodes:
    if not node.has_chunk(chunk_hash):
      // Node is missing this chunk — replicate from primary
      data = primary_loc.node.retrieve(chunk_hash)
      node.store(chunk_hash, data)
      log "Replicated chunk {chunk_hash} to {node.name}"
```

In the simulation, replication is represented by the 3-way `quorum_write` which stores to all selected nodes atomically. Cross-region async replication is simulated by the dotted edges between storage nodes in the architecture diagram.

---

## Algorithm Summary

| Algorithm | File | Key Method | Complexity | Purpose |
|-----------|------|------------|------------|---------|
| Rabin Fingerprint CDC | `src/core/chunking.py` | `chunk()` | O(n) | Variable-size file chunking |
| SHA-256 Dedup | `src/core/dedup.py` | `store()` | O(1) avg | Global duplicate elimination |
| Incremental Backup | `src/backup/engine.py` | `run_backup()` | O(bytes + chunks) | Changed-data-only backup |
| Point-in-Time Restore | `src/restore/engine.py` | `_restore_file()` | O(chunks) | Version chain reconstruction |
| Quorum Write | `src/storage/cluster.py` | `quorum_write()` | O(N) | Durable distributed storage |
| Quorum Read | `src/storage/cluster.py` | `quorum_read()` | O(1) avg | Fault-tolerant chunk retrieval |
| Exponential Backoff | (retry pattern) | — | O(retries) | Transient failure handling |
| Garbage Collection | (production only) | — | O(candidates) | Dead chunk reclamation |
