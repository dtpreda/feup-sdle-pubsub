mod server;

use clap::Parser;
use tracing::Level;

use crate::server::Server;

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value_t = 5555)]
    port: u16,
}

fn main() -> Result<(), zmq::Error> {
    let args = Args::parse();

    let collector = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(collector).expect("failed to set global logger");

    let mut server = Server::new(args.port)?;
    server.run();
    Ok(())
}
