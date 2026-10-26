
# ForgeKV — Project Instructions

## 1. Project Vision

ForgeKV is an educational, persistent key-value storage engine written in Rust.

The goal is to understand and implement core database internals:
- Write-ahead logging (WAL)
- In-memory indexing and memtables
- Log-structured merge trees (LSM-trees)
- Sorted string tables (SSTables)
- Compaction and tombstones
- Crash recovery and durability
- Asynchronous TCP networking using Tokio
- Concurrent request handling and backpressure
- Reproducible performance benchmarking

This project should demonstrate strong systems programming, storage-engine design, testing, and performance analysis.

## 2. Engineering Principles

- Prefer correctness and data integrity over premature optimization.
- Use safe Rust by default. Avoid unsafe code unless there is a compelling, documented reason.
- Keep modules small, cohesive, and testable.
- Use clear abstractions and meaningful error types.
- Never silently ignore I/O errors or corrupt database records.
- Document important invariants and non-obvious design decisions.
- Avoid unnecessary dependencies.
- Do not implement features before their architectural dependencies are ready.
- Never claim a feature or performance result is complete without evidence.

## 3. Technology Stack

- Language: Rust
- Async runtime: Tokio
- Storage: Standard filesystem APIs initially
- Serialization: A simple, explicitly specified binary format where appropriate
- Testing: Rust unit tests, integration tests, and fault/recovery tests
- Benchmarking: Criterion or a justified custom benchmark harness
- Networking: Tokio TCP

Choose additional dependencies only when justified and explain their purpose.

## 4. Architecture Goals

The intended architecture may include:

- `engine`: public key-value API and coordination
- `wal`: append-only write-ahead log and replay
- `memtable`: in-memory key-value index
- `sstable`: sorted immutable on-disk tables
- `manifest`: persistent metadata describing storage files and versions
- `compaction`: merging SSTables and removing obsolete records safely
- `protocol`: request/response encoding for TCP clients
- `server`: asynchronous TCP server and connection handling
- `benchmark`: repeatable workload generation and measurements

These are proposed boundaries, not mandatory modules. Refine them during planning and explain significant changes.

## 5. Data Integrity and Durability

- Define what a successful write guarantees.
- Define behavior for process crashes and partial WAL records.
- Make recovery deterministic and testable.
- Specify record framing, lengths, encoding, and versioning.
- Handle deletes with tombstones so deleted keys do not reappear from older SSTables.
- Ensure compaction and file replacement cannot silently lose acknowledged data.
- Treat filesystem sync behavior and durability trade-offs explicitly.
- Test restart and recovery behavior, not just normal operations.

Do not claim power-loss durability unless the required filesystem synchronization and metadata ordering have been implemented and tested appropriately.

## 6. Testing Requirements

Every phase must include relevant tests.

At minimum, cover:
- Basic put, get, and delete behavior
- Reopening the database after writes
- WAL replay and incomplete trailing records
- Overwrites and deletes across storage levels
- Compaction correctness
- Corrupt or invalid input handling
- Concurrent client behavior
- Recovery after simulated failures
- Regression tests for every discovered bug

Use deterministic tests where possible. Never remove a failing test simply to make the suite pass.

## 7. Performance Requirements

Measure performance rather than inventing claims.

Report relevant workload details, including:
- Hardware and operating environment
- Dataset size and key/value sizes
- Read/write workload distribution
- Concurrency level
- Throughput in operations per second
- p50, p95, and p99 latency
- Benchmark duration and warm-up procedure

Keep correctness tests separate from performance benchmarks. Avoid comparing results unless the workloads and measurement methods are comparable.

## 8. Development Workflow

Work in explicit phases.

For each phase:
1. Explain the goal and design.
2. List the files and interfaces to create or modify.
3. Identify invariants, edge cases, and failure modes.
4. Propose a test plan.
5. Implement only the approved scope.
6. Run formatting, tests, and relevant checks.
7. Summarize changed files, results, limitations, and next steps.

Do not implement the entire project in one pass.

Before implementation begins, inspect the existing repository and prepare a phased plan. Wait for approval before making substantial code changes.

Prefer small, reviewable changes. Do not overwrite existing work without checking it first.

## 9. Rust Quality Standards

- Run `cargo fmt --check`.
- Run `cargo test`.
- Run `cargo clippy --all-targets --all-features -- -D warnings` where practical.
- Use `Result` and meaningful error types for fallible operations.
- Avoid unnecessary cloning and unbounded buffering.
- Keep concurrency and locking decisions explicit.
- Explain trade-offs involving ownership, synchronization, and async execution.
- Do not use `unwrap()` or `expect()` in production paths without a clearly justified invariant.

## 10. Definition of Done

A feature is complete only when:
- Its behavior is implemented.
- Relevant tests pass.
- Failure cases are considered.
- Its limitations are documented.
- The implementation matches the project's durability and concurrency guarantees.
- Performance claims, if any, have reproducible supporting measurements.

## 11. Scope Control

The first release should be a small, understandable storage engine, not a production replacement for RocksDB or another mature database.

Avoid premature additions such as distributed replication, SQL, consensus protocols, transactions, or a query language.

Prioritize a correct single-node database, then its networking layer, then measurement and hardening.

When a requirement is ambiguous, explain the options and recommend a simple, defensible design before proceeding.
