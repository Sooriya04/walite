# walite Engineering Roadmap: 5 Phases & Testing Architecture

## 1. Overview
This roadmap establishes the phased milestones for `walite`. To preserve architectural continuity across sessions and contributors, each phase defines its deliverables, file boundaries, and verification suites.

Testing layout convention:
- Unit, integration, module, and benchmark test suites are organized in dedicated test files under `test/` (e.g., `test/record_test.go`, `test/segment_test.go`, `test/partition_test.go`, `test/smoke_test.go`).
- Every test file adheres to the mandatory 5-line technical header comment.

---

## 2. Phase Breakdown

### Phase 1: Bare-Metal Storage Engine (Log & Segment Core)
- **Objective:** Deterministic, crash-safe, append-only binary storage on disk with zero external dependencies.
- **Components:**
  - `internal/log/record.go`: Fixed 28-byte binary layout + IEEE CRC32 checksum engine.
  - `internal/log/index.go`: Sparse index entry (`[RelativeOffset: 4B][Position: 4B]`) with binary search.
  - `internal/log/segment.go`: Immutable segment rotation (`MaxSegmentBytes`), active writer head, and crash recovery.
- **Testing (`test/`):**
  - `test/record_test.go`:
    - Unit tests: Roundtrip serialization for keyless and keyed records.
    - Fuzzing & bit-flip integrity: Verify every bit alteration returns `ErrCorruptRecord`.
    - Truncated slice safety: Prevent out-of-bounds panics on incomplete streams.
  - `test/segment_test.go`:
    - Module tests: Sparse index lookup precision across 4KB intervals.
    - Rollover validation: Auto-closing active segment when size limit is breached.
    - Torn-write recovery: Truncating corrupt partial writes upon initialization.

### Phase 2: Partition Management & Durability Controls
- **Objective:** High-watermark concurrency, lock-free tail reads, and configurable disk flush policies.
- **Components:**
  - `internal/log/partition.go`: Active head segment management and historical read-only segment list.
  - `internal/log/flusher.go`: Durability modes (`SyncAlways`, `SyncBatch`, `SyncInterval`).
- **Testing (`test/`):**
  - `test/partition_test.go`:
    - Integration tests: Multiple producers appending concurrently alongside concurrent readers (`go test -race`).
    - Crash-restart durability: Abrupt process termination without graceful close, verifying zero data loss.
    - Retention policies: Segment garbage collection based on time and byte limits.

### Phase 3: Consumer Groups & Offset Checkpointing
- **Objective:** Consumer group state management without external databases.
- **Components:**
  - `internal/consumer/offset_manager.go`: Checkpoint log for consumer group offsets (`group_id -> topic -> partition -> offset`).
  - `internal/consumer/rebalance.go`: Partition assignment strategies and consumer heartbeats.
- **Testing (`test/`):**
  - `test/consumer_test.go`:
    - Integration tests: Offset commit persistence across restarts.
    - Duplicate prevention: Verifying consumers resume at exact next offsets.
    - Group rebalancing: Simulating consumer leave/join events.

### Phase 4: Wire Protocol & walited Daemon
- **Objective:** High-throughput, low-latency binary TCP server.
- **Components:**
  - `internal/protocol/codec.go`: Zero-copy binary framing (`[Len: 4B][Magic: 2B][Version: 1B][OpCode: 1B]`).
  - `cmd/walited/main.go`: Network listener, connection pool, and graceful termination.
- **Testing (`test/`):**
  - `test/protocol_test.go`: Frame codec validation, malformed magic rejection, payload slicing.
  - `test/smoke_test.go`:
    - End-to-end smoke test: Boot daemon on ephemeral TCP port, produce batches, consume back, verify offsets, and cleanly shut down within 2 seconds.

### Phase 5: Client SDK (`pkg/walite`) & CLI Tooling (`cmd/walite`)
- **Objective:** Public embedded library and CLI operator diagnostics.
- **Components:**
  - `pkg/walite/`: Public Go package exposing `walite.Open()` (embedded) and `walite.Dial()` (network).
  - `cmd/walite/`: CLI utility to inspect segments, verify CRC checksums, and run latency benchmarks.
- **Testing (`test/`):**
  - `test/sdk_test.go`: Public API lifecycle tests and embedded engine verification.
  - `test/benchmark_test.go`:
    - Regression benchmarks: `BenchmarkAppend` verifying zero heap allocations in the hot path (`0 B/op, 0 allocs/op`).
