# AGENT.md

## Repository Overview
`walite` is an ultra-lightweight, zero-dependency, crash-resilient write-ahead log (WAL) and partitioned commit stream engine written in pure Go. It delivers micro-Kafka semantics for embedded systems, single-node services, and edge computing environments without external dependencies or heavy JVM/KRaft runtimes.

## Maintainers & Collaboration
- **Architect & Agent Maintainer:** Barbara Vane (`barbaravane`)
- **Core Contributor & Systems Engineer:** Sooriya
- **Collaboration Model:** Feature-branch and Pull Request workflow. Architectural specifications are documented first. Implementation proceeds incrementally with review comments, benchmarks, and test verification on every PR.

## Codebase Standards & Invariants
1. **Mandatory File Header (Lines 1-5):**
   Every source file (`.go`, `.proto`, scripts, etc.) must begin with a concise comment block stating its responsibility and key exported functions/types:
   ```go
   // Package: <package_name>
   // File: <filename>
   // Core Work: <1-2 sentences on primary responsibility>
   // Exported: <Key structs, interfaces, and primary functions>
   ```
2. **Zero Emojis & Professional Tone:**
   Code files, test cases, error messages, and documentation must remain strictly technical, clear, and professional.
3. **Zero Third-Party Runtime Dependencies:**
   The core engine (`pkg/walite`, `internal/*`) must rely solely on the Go standard library (`os`, `sync`, `encoding/binary`, `hash/crc32`, `io`, `net`).
4. **Crash Durability & Corruption Resistance:**
   - Every record is CRC32-checksummed.
   - Segment boundary checks and sparse indexing allow rapid crash recovery.
   - Explicit `Sync()` policies (`Always`, `Batch`, `Interval`) guarantee durability guarantees.
5. **Memory Safety & Zero-Allocation Path:**
   Hot append and read loops must minimize allocations using buffer pooling (`sync.Pool`) and direct slice slices.

## Ponytail Plugin & Skills Integration
The repository integrates the `ponytail` engineering governance suite in `.agent/skills/`:
- `ponytail-review`: Automated PR reviews, code quality checks, and architectural alignment.
- `ponytail-audit`: Security auditing, bounds verification, and concurrency race inspection.
- `ponytail-debt`: Technical debt and allocation hotspot tracking.
- `ponytail-gain`: Performance gains and benchmark regression analysis.
- `ponytail-help`: Command and workflow assistance.
