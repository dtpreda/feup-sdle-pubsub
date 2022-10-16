mod client;

use clap::Parser;
use client::OperationType;

use crate::client::Client;

#[derive(clap::Parser)]
struct Args {
    #[command(subcommand)]
    /// The operation to execute on the service.
    operation: OperationCommand,

    #[arg(short, long)]
    /// The topic this action is related to.
    topic: String,

    #[arg(short, long)]
    /// The client id used for subscriptions.
    id: String,

    #[arg(short, long, default_value_t = String::from("tcp://localhost:5555"))]
    /// The service url.
    url: String,
}

#[derive(clap::Subcommand, Debug)]
enum OperationCommand {
    #[command(name = "subscribe")]
    Subscribe,

    #[command(name = "unsubscribe")]
    Unsubscribe,

    #[command(name = "put")]
    Put {
        #[arg(short, long)]
        message: String,
    },

    #[command(name = "get")]
    Get,
}

fn main() -> Result<(), zmq::Error> {
    let args = Args::parse();

    let (operation_type, message): (OperationType, Option<String>) = match args.operation {
        OperationCommand::Subscribe => (OperationType::Subscribe, None),
        OperationCommand::Unsubscribe => (OperationType::Unsubscribe, None),
        OperationCommand::Put { message } => (OperationType::Put, Some(message)),
        OperationCommand::Get => (OperationType::Get, None),
    };

    let mut client = Client::new(operation_type, args.id, args.url, args.topic, message)?;

    client.execute();

    Ok(())
}
