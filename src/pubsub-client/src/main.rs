mod client;

use clap::{command, Parser, Subcommand};
use client::perform_operation;

#[derive(Parser, Debug)]
struct Args {
    /// The desired operation.
    #[command(subcommand)]
    operation: Operation,

    /// The ID of the client.
    #[arg(short, long, env = "CLIENT_ID")]
    id: String,

    /// The URL of the server.
    #[arg(
        short,
        long,
        env = "SERVER_URL",
        default_value = "tcp://localhost:5555"
    )]
    url: String,
}

#[derive(Subcommand, Debug)]
#[command(infer_subcommands = true)]
pub enum Operation {
    Put {
        /// The topic to publish to.
        topic: String,

        /// The message to publish (read from standard input).
        message: String,
    },
    Get {
        /// Topic to fetch.
        topic: String,
    },
    Subscribe {
        /// Topic to subscribe to.
        topic: String,
    },
    Unsubscribe {
        /// Topic to unsubscribe from.
        topic: String,
    },
}

fn main() -> Result<(), zmq::Error> {
    let args = Args::parse();
    if args.id.contains('/') || args.id.contains('-') {
        eprintln!("Client ID must not contain '/' or '-'");
        std::process::exit(1);
    }
    let topic = match &args.operation {
        Operation::Put { topic, .. } => topic,
        Operation::Get { topic } => topic,
        Operation::Subscribe { topic } => topic,
        Operation::Unsubscribe { topic } => topic,
    };
    if topic.contains('/') || topic.contains('-') {
        eprintln!("Topic must not contain '/' or '-'");
        std::process::exit(1);
    }
    perform_operation(args.id, args.url, args.operation)?;
    Ok(())
}
