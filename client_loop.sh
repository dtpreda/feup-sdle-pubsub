#!/bin/sh
DIR=$(dirname "$0")
while cargo run -p pubsub-client --manifest-path "$DIR/src/Cargo.toml" --target-dir "$DIR/src/target" -q -- "$@"; do
        echo ""
        echo ""
        sleep 1
done