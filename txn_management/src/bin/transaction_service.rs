use proto::cluster_management_net::cluster_management_service_server::ClusterManagementServiceServer;
use replication::channel_pool::ChannelPool;
use std::env;
use tonic::transport::Server;
use txn_management::{cluster_management_service::ClusterManager, NodeStatus};

#[tokio::main]
async fn main() {
    // <Cluster leader addrs>, pred, succ, my cluster addr, my config addr, [0: head | 1: middle | 2: tail]
    let mut args: Vec<String> = env::args().skip(1).collect();
    let my_node_type = args.pop().unwrap().parse::<u32>().unwrap();
    let my_config_addr = args.pop().unwrap().parse().unwrap();
    let my_cluster_addr = args.pop().unwrap().parse().unwrap();

    let node_status = match my_node_type {
        0 => {
            let succ = args.pop().unwrap().parse().unwrap();
            NodeStatus::new_head(succ)
        }
        1 => {
            let succ = args.pop().unwrap().parse().unwrap();
            let pred = args.pop().unwrap().parse().unwrap();
            NodeStatus::new_middle(pred, succ)
        }
        2 => {
            let pred = args.pop().unwrap().parse().unwrap();
            NodeStatus::new_tail(pred)
        }
        _ => panic!("Unknown node type"),
    };
    let num_shards = u32::try_from(args.len()).unwrap();
    let conn_pool = ChannelPool::new(None, args);
    let cluster_manager = ClusterManager::new(node_status, my_cluster_addr, num_shards, conn_pool);
    let cluster_manager_service = ClusterManagementServiceServer::new(cluster_manager);
    if let Err(_) = Server::builder()
        .add_service(cluster_manager_service)
        .serve(my_config_addr)
        .await
    {
        panic!("Cluster management service failed to initialize")
    }
}
