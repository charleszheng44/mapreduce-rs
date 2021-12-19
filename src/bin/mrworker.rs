use anyhow::Result;
use mapreduce_rs::mr::worker;

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args();
    if args.len() < 2 {
        eprintln!("Usage: mrworker xxx.so");
        std::process::exit(1);
    }

    let libpath = std::env::args()
        .nth(1)
        .expect("failed to get the library path");

    worker::start_worker(libpath).await?;
    println!("mrworker is running...");

    Ok(())
}
