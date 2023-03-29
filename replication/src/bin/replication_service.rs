use proto::cluster_management_net::cluster_management_service_server::ClusterManagementServiceServer;
use replication::{
    channel_pool::ChannelPool, cluster_management_service::ClusterManager, TAIL_NID,
};
use std::env;
use tonic::transport::Server;

#[tokio::main]
async fn main() {
    // tail addr, <Cluster addrs>, my cluster addr, my config address, my node id, my shard id
    // Take nodeId 1 to be statically configured leader
    let mut args: Vec<String> = env::args().skip(1).collect();
    let shard_id = args.pop().unwrap().parse::<u32>().unwrap();
    let node_id = args.pop().unwrap().parse().unwrap();
    assert!(node_id != TAIL_NID, "tail id must be 0");
    let my_config_addr = args.pop().unwrap().parse().unwrap();
    let my_cluster_addr = args.pop().unwrap().parse().unwrap();
    let conn_pool = ChannelPool::new(Some(node_id), args);

    let cluster_manager = ClusterManagementServiceServer::new(ClusterManager::new(
        shard_id,
        node_id,
        my_cluster_addr,
        conn_pool,
    ));

    if let Err(_) = Server::builder()
        .add_service(cluster_manager)
        .serve(my_config_addr)
        .await
    {
        panic!("Cluster management service failed to initialize")
    }
}

// TODO(med priority): Tests
