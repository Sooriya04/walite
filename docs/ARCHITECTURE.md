# walite Architecture Specification

## 1. Overview
`walite` provides high-throughput, ordered, append-only log streams partitioned across logical topics. It bridges the gap between simple SQLite WAL internals and distributed commit logs like Apache Kafka, optimized for single-machine saturation.

## 2. Directory Structure
```
walite/
├── cmd/
│   ├── walited/          # Standalone daemon with lightweight binary/TCP protocol
│   └── walite/           # CLI tool for log inspection, dumping, and benchmarking
├── pkg/
│   └── walite/           # Public Go client and embedded log engine API
├── internal/
│   ├── log/              # Storage engine: segments, active write head, index, recovery
│   ├── protocol/         # Zero-copy binary framing protocol for network transport
│   └── consumer/         # Consumer group cursor tracking and offset checkpointing
├── docs/                 # Architecture, wire protocol, and benchmark docs
├── .agent/               # Ponytail engineering governance skills and rules
├── AGENT.md              # Contributor guidelines and codebase invariants
└── go.mod
```

## 3. Storage Model

### 3.1 Partition Directory Layout
Each topic partition is represented as a directory on the filesystem:
```
data/
└── <topic>/
    └── partition-<id>/
        ├── 00000000000000000000.log
        ├── 00000000000000000000.index
        ├── 00000000000000100450.log
        ├── 00000000000000100450.index
        └── active.meta
```

### 3.2 Segment File Format (.log)
A segment file consists of contiguous binary records. Each record layout is structured as follows:

```
+-----------------------------------------------------------------------+
| Offset (8B) | Timestamp (8B) | CRC32 (4B) | KeyLen (4B) | ValLen (4B) |
+-----------------------------------------------------------------------+
| Key Bytes (Variable)         | Value Bytes (Variable)                 |
+-----------------------------------------------------------------------+
```

- **Offset** (`uint64`, 8 bytes, Big-Endian): Monotonically increasing 64-bit integer.
- **Timestamp** (`int64`, 8 bytes, Big-Endian): Unix nanosecond timestamp when accepted.
- **CRC32** (`uint32`, 4 bytes, Big-Endian): IEEE checksum computed over `Offset + Timestamp + KeyLen + ValLen + Key + Value`.
- **KeyLen** (`uint32`, 4 bytes, Big-Endian): Length of key in bytes (0 if keyless).
- **ValLen** (`uint32`, 4 bytes, Big-Endian): Length of payload in bytes.
- **Key Bytes**: Raw slice.
- **Value Bytes**: Raw slice.

### 3.3 Sparse Index Format (.index)
Instead of indexing every record, `walite` maintains a sparse index entry every $N$ bytes (default: 4096 bytes) to keep index files small enough to be fully memory-mapped (`mmap`) or resident in OS page cache:

```
+------------------------------------+
| Relative Offset (4B) | Position (4B)|
+------------------------------------+
```
- **Relative Offset** (`uint32`, 4 bytes): Segment-relative offset (`Offset - BaseOffset`).
- **Position** (`uint32`, 4 bytes): Byte offset from the start of the `.log` file.

Binary search is executed across the index in $O(\log K)$ time, followed by linear scan of at most 4KB in the log file to reach the exact target offset.

## 4. Durability & Concurrency Model
1. **Single Writer, Multiple Readers**:
   - Each partition has exactly one active segment writer protected by a Mutex/RWMutex.
   - Readers can tail segments concurrently using file reads without blocking active appends up to the high-watermark offset.
2. **Sync Policies**:
   - `SyncAlways`: Immediate `fdatasync()` per write. High durability, capped by disk IOPS.
   - `SyncBatch`: `fdatasync()` triggered after $M$ records or $B$ bytes accumulated.
   - `SyncInterval`: Background flusher running on periodic ticks (e.g., 50ms).
