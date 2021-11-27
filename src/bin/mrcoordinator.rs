use mapreduce_rs::mr::coordinator as cd;
use mapreduce_rs::mr::coordinator::mr_types::coordinator_server::CoordinatorServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args();
    if args.len() < 2 {
        eprintln!("Usage: mrcoordinator inputfiles...");
        std::process::exit(1);
    }
    let files = args.skip(1).collect::<Vec<String>>();
    let coordinator = cd::MutexMRCoordinator::new(files, 10);

    let addr = "[::1]:50051".parse()?;

    println!("coordinator is running...");
    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}
