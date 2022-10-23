# SDLE First Assignment

SDLE First Assignment of group T5G13.

Group members:

1. Bruno Mendes (up201906166@up.pt)
2. David Preda (up201904726@up.pt)
3. Joaquim Monteiro (up201905257@up.pt)

## How to run

You can run the `pubsub-server` and `pubsub-client` binaries directly with `cargo`, inside [src](src/).

We have provided utility scripts for launching the server and the clients. For usage information, run `server.sh --help` and `client.sh --help`.

The `client_loop.sh` script simulates a client background daemon, by launching clients repeatedly performing the same action. You can, for instance, get from a topic to get new messages in real time.

> Beware that `cargo run` will operate silently, e.g., in a non-verbose mode, so that you can pipe the output of the programs if you need to. If the source code is not yet compiled on your machine, it might take a while for anything to happen. Just wait for some seconds as `cargo` compiles the application.

## The implementation

For details about our implementation choices and system architecture, please check the report at [doc](doc/).