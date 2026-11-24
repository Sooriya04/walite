# walite Phase 4 & 5 Engineering Tasks

## Repository Status
- **Maintainers:** Barbara Vane (`barbaravane`), Sooriya (`Sooriya04`)
- **Active Phase:** Phase 4 (Wire Protocol & TCP Daemon)
- **Target Invariants:** Zero external dependencies (pure Go stdlib), mandatory 5-line technical headers, function manifests, zero-alloc hot path, and crash durability.

---

## Phase 4 Task Breakdown & Live Status

### Task 4.1: Wire Protocol Binary Framing & Codec
- **Status:** COMPLETED (Commit `30c3a6c`)
- **Deliverables:**
  - `internal/protocol/codec.go`: Length-prefixed binary framing layout:
    `[FrameLen: 4B][Magic: 2B (0x574C)][Version: 1B (0x01)][OpCode: 1B][Payload (FrameLen - 4B)]`
  - Zero-allocation header pooling via `sync.Pool`.
  - Boundary guards: Hard 16MB frame ceiling (`MaxFrameBytes`), `ErrInvalidMagic`, `ErrUnsupportedVersion`, `ErrInvalidFrameLen`, `ErrTruncatedFrame`.
  - `internal/protocol/messages.go`: Typed binary request and response serializers for:
    - `0x01` / `0x81`: `PRODUCE_REQ` / `PRODUCE_RESP`
    - `0x02` / `0x82`: `CONSUME_REQ` / `CONSUME_RESP`
    - `0x03` / `0x83`: `OFFSET_REQ` / `OFFSET_RESP`
    - `0x04` / `0x84`: `COMMIT_REQ` / `COMMIT_RESP`
    - `0x05` / `0x85`: `PING_REQ` / `PONG_RESP`

### Task 4.2: Codec Verification Suite & Transport Benchmarks
- **Status:** COMPLETED
- **Deliverables:**
  - `test/protocol_test.go`:
    - `TestProtocol_FrameRoundtrip`: Validates empty, small, and multi-KB payloads.
    - `TestProtocol_InvalidMagic`: Verifies rejection of non-0x574C magic bytes.
    - `TestProtocol_UnsupportedVersion`: Enforces protocol version 0x01.
    - `TestProtocol_FrameTooLarge`: Blocks packets exceeding 16MB ceiling.
    - `TestProtocol_TruncatedFrame`: Clean EOF handling without panics or leaks.
    - `TestProtocol_ProduceRoundtrip`: Serializes/deserializes multi-record batches with CRC32.
    - `TestProtocol_ConsumeRoundtrip`: Offset-bounded record fetch validation.
    - `TestProtocol_OffsetRoundtrip` & `TestProtocol_CommitRoundtrip`: Consumer group checkpoint wire verification.
    - `TestProtocol_PingPongRoundtrip`: Echo heartbeat verification.
    - `TestProtocol_MalformedPayloads`: Slicing boundary and truncated stream safety.
    - `TestProtocol_NetPipeTransport`: Validates bi-directional streaming over `net.Pipe()`.
    - `BenchmarkProtocol_EncodeFrame` & `BenchmarkProtocol_ReadFrame`: Benchmarks asserting zero-alloc hot path.

### Task 4.3: High-Throughput `walited` TCP Server Daemon
- **Status:** COMPLETED
- **Components:**
  - `internal/server/config.go`: Sane defaults, connection timeouts, and partition configuration.
  - `internal/server/server.go`:
    - Pure Go stdlib `net.Listener` concurrent TCP server.
    - Central routing registry for active `log.Partition` instances and consumer `OffsetManager`.
    - Per-connection worker goroutines handling framed streams for `PRODUCE`, `CONSUME`, `OFFSET_GET`, `OFFSET_COMMIT`, and `PING`.
    - Dynamic partition auto-provisioning and graceful drain (<2s) syncing storage to disk.
  - `cmd/walited/main.go`:
    - CLI entrypoint, flag parsing (`--addr`, `--dir`, `--flush`, `--max-segment`, `--sync`).
    - OS signal handler (`SIGINT`, `SIGTERM`) triggering graceful server shutdown.
  - `internal/server/server_test.go`:
    - Integration tests covering startup/teardown, produce batch and consume, offset commit/query, and partition missing error handling.

### Network Lifecycle & Protocol Hardening (Audit Fixes)
- **Status:** COMPLETED
- **Commits:** `6923fb8`, `fa91b64`, `33b1753`, `5d6a1ea`, pending
- **Deliverables:**
  - Bug 1 (Commit `6923fb8`): Track active client sockets in `Server.conns` with atomic shutdown coordination, closing sockets immediately on shutdown to prevent goroutine leaks and eliminate the 2s shutdown hang.
  - Bug 2 (Commit `fa91b64`): Enforce per-frame `conn.SetReadDeadline` and `writeResponse` `conn.SetWriteDeadline` in TCP worker goroutines to prevent Slowloris socket exhaustion.
  - Bug 3 (Commit `33b1753`): Map `io.ErrUnexpectedEOF` during header and payload reads in `protocol.ReadFrame` to `protocol.ErrTruncatedFrame`, preserving `io.EOF` for clean stream terminations.
  - Bug 4 (Commit `5d6a1ea`): Return StatusErrServerClosed (0x0006) on produce, consume, offset-get, and offset-commit during server shutdown instead of generic StatusErrUnknown or StatusErrPartitionNotFound.
  - Bug 5: Clamp string field lengths to 65,535 bytes in message serialization to prevent 16-bit integer overflow and frame payload corruption.

### Task 4.4: End-to-End Integration Smoke Test
- **Status:** PENDING
- **Deliverables:**
  - `test/smoke_test.go`:
    - Boots `walited` on an ephemeral TCP port (`127.0.0.1:0`).
    - Produces batches concurrently, consumes back, and validates offsets and CRC integrity.
    - Commits consumer group offsets, disconnects, reconnects, and asserts seamless cursor resumption.
    - Triggers graceful shutdown and asserts zero torn writes.

---

## Phase 5 Task Breakdown (Ahead)

### Task 5.1: Client SDK (`pkg/walite/`)
- Public Go client library supporting `walite.Open()` (embedded engine) and `walite.Dial()` (network TCP client).
- Idiomatic Producer and Consumer group APIs.

### Task 5.2: CLI Diagnostics Utility (`cmd/walite/`)
- Segment inspector, CRC32 verifier, and throughput latency benchmarking CLI.
