#!/usr/bin/env bash

set -e
cd "$(dirname "$0")/.."

source ./ci/rust-version.sh stable

export RUSTFLAGS="-D warnings"
export RUSTBACKTRACE=1

set -x

# Build/test all host crates
cargo +"$rust_stable" build

sudo sysctl -w fs.file-max=500000
sudo sysctl -p

sudo cargo +"$rust_stable" test -- --nocapture

exit 0
