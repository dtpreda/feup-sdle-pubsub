mod client;

use clap::Parser;

use crate::client::Client;

#[derive(clap::Parser, Debug)]
struct Args {
    #[command(subcommand)]
    operation: Operation,
}

#[derive(clap::Subcommand, Debug)]
enum Operation {
    #[command(name = "subscribe")]
    Subscribe(TopicOperationArgs),
    #[command(name = "unsubscribe")]
    Unsubscribe(TopicOperationArgs),
    #[command(name = "put")]
    Put(MessageOperationArgs),
    #[command(name = "get")]
    Get(MessageOperationArgs),
}

// The common args should really be up in the Args struct, not sure why it does not work there
#[derive(clap::Args, Debug)]
struct TopicOperationArgs {
    #[arg(short, long)]
    topic: String,
    #[arg(short, long)]
    id: String,
    #[arg(short, long, default_value_t = 5555)]
    port: u16,
}

#[derive(clap::Args, Debug)]
struct MessageOperationArgs {
    #[arg(short, long)]
    topic: String,
    #[arg(short, long)]
    message: String,
    #[arg(short, long)]
    id: String,
    #[arg(short, long, default_value_t = 5555)]
    port: u16,
}

fn main() {
    let args = Args::parse();
    println!("{:#?}", args);

    let mut client: Client = match args.operation {
        Operation::Subscribe(args) => Client::new(
            client::OperationType::Subscribe,
            args.id,
            args.port,
            args.topic,
            None,
        )
        .unwrap(),
        Operation::Unsubscribe(args) => Client::new(
            client::OperationType::Unsubscribe,
            args.id,
            args.port,
            args.topic,
            None,
        )
        .unwrap(),
        Operation::Put(args) => Client::new(
            client::OperationType::Put,
            args.id,
            args.port,
            args.topic,
            Option::Some(args.message),
        )
        .unwrap(),
        Operation::Get(args) => Client::new(
            client::OperationType::Put,
            args.id,
            args.port,
            args.topic,
            Option::Some(args.message),
        )
        .unwrap(),
    };

    client.execute();
}
