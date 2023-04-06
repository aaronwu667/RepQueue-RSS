use proto::chain_management_net::chain_management_service_server::ChainManagementServiceServer;
use std::env;
use tonic::transport::Server;
use txn_management::chain_management_service::ChainManager;

#[tokio::main]
async fn main() {
    // my cluster addr, my config addr
    let mut args: Vec<String> = env::args().skip(1).collect();
    let my_config_addr = args.pop().unwrap();
    let my_cluster_addr = args.pop().unwrap();

    let cluster_manager = ChainManager::new(my_cluster_addr);
    let cluster_manager_service = ChainManagementServiceServer::new(cluster_manager);
    if Server::builder()
        .add_service(cluster_manager_service)
        .serve(my_config_addr.parse().unwrap())
        .await
        .is_err()
    {
        panic!("Cluster management service failed to initialize")
    }
}
