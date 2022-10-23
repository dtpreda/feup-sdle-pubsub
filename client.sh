#!/bin/sh
cd src && cargo run --bin pubsub-client -q -- "$@" && cd ..