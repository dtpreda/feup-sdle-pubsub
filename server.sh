#!/bin/sh
cd src && cargo run --bin pubsub-server -q -- "$@" && cd ..