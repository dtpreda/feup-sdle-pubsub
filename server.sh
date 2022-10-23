#!/bin/sh
DIR=$(dirname "$0")
cargo run -p pubsub-client --manifest-path "$DIR/src/Cargo.toml" --target-dir "$DIR/src/target" -q -- "$@"