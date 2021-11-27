use mapreduce_rs::mr::coordinator as cd;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args();
    if args.len() < 2 {
        eprintln!("Usage: mrcoordinator inputfiles...");
        std::process::exit(1);
    }
    let files = args.skip(1).collect::<Vec<String>>();

    println!("coordinator is running...");
    cd::MutexMRCoordinator::run(files, 10).await?;
    Ok(())
}
