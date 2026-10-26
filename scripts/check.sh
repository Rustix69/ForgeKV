#!/usr/bin/env bash
# Formatting, lint, and test gate. Run from anywhere; operates on the repo root.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
# Crash/failpoint tests only exist with the feature enabled.
cargo test --all-targets --all-features
