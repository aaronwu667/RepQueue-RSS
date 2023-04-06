use proto::cluster_management_net::cluster_management_service_server::ClusterManagementServiceServer;
use replication::cluster_management_service::ClusterManager;
use std::env;
use tonic::transport::Server;

#[tokio::main]
async fn main() {
    // my cluster addr, my config address
    let mut args: Vec<String> = env::args().skip(1).collect();
    let my_config_addr = args.pop().unwrap();
    let my_cluster_addr = args.pop().unwrap();

    let cluster_manager = ClusterManagementServiceServer::new(ClusterManager::new(my_cluster_addr));

    if Server::builder()
        .add_service(cluster_manager)
        .serve(my_config_addr.parse().unwrap())
        .await
        .is_err()
    {
        panic!("Cluster management service failed to initialize")
    }
}

// TODO(med priority): Tests
