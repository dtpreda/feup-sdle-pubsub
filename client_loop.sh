#!/bin/sh
cd src

while true
do
    if cargo run --bin pubsub-client -q -- "$@"; then
        echo ""
        echo ""
        sleep 1
    else
        break
    fi
done

cd ..