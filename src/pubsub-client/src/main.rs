use clap::Parser;

#[derive(clap::Parser, Debug)]
struct Args {
    #[command(subcommand)]
    mode: Mode,
    #[arg(short, long, default_value_t = 5555)]
    port: u16,
}

#[derive(clap::Subcommand, Debug)]
enum Mode {
    #[command(name = "pub")]
    Publisher,
    #[command(name = "sub")]
    Subscriber,
}

fn main() {
    let args = Args::parse();
    println!("{:#?}", args);
}
