mod server;

use clap::Parser;

use crate::server::Server;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value_t = 5555)]
    port: u16,
}

fn main() -> Result<(), zmq::Error> {
    let args = Args::parse();
    let mut server = Server::new(args.port)?;
    server.run();
    Ok(())
}
