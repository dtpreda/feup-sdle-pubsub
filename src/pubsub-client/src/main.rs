mod client;

use clap::{command, Parser, Subcommand};
use client::service_execute;

#[derive(Parser, Debug)]
struct Args {
    /// The URL of the server.
    #[arg(
        short,
        long,
        env = "SERVER_URL",
        default_value = "tcp://localhost:5555"
    )]
    url: String,

    /// The desired operation.
    #[command(subcommand)]
    operation: Operation,
}

#[derive(Subcommand, Debug)]
#[command(infer_subcommands = true)]
pub enum Operation {
    Put {
        /// The topic to publish to.
        #[arg(short, long)]
        topic: String,

        /// The message to publish (read from standard input).
        #[arg(short, long)]
        message: String,
    },
    Get {
        /// The ID of the subscriber.
        #[arg(short, long, env = "SUBSCRIBER_ID")]
        id: String,

        /// Topic to fetch.
        #[arg(short, long)]
        topic: String,
    },
    Subscribe {
        /// The ID of the subscriber.
        #[arg(short, long, env = "SUBSCRIBER_ID")]
        id: String,

        /// Topic to subscribe to.
        #[arg(short, long)]
        topic: String,
    },
    Unsubscribe {
        /// The ID of the subscriber.
        #[arg(short, long, env = "SUBSCRIBER_ID")]
        id: String,

        /// Topic to unsubscribe from.
        #[arg(short, long)]
        topic: String,
    },
}

fn main() -> Result<(), zmq::Error> {
    let args = Args::parse();
    service_execute(args.url, args.operation)?;
    Ok(())
}
