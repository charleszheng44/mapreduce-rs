use mapreduce_rs::mr::worker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args();
    if args.len() < 2 {
        eprintln!("Usage: mrworker xxx.so");
        std::process::exit(1);
    }

    worker::start_worker().await?;
    println!("mrworker is running...");

    Ok(())
}
