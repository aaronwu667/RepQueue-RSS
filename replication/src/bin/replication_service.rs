use openraft::{Config, Raft};
use proto::cluster_management_net::cluster_management_service_server::ClusterManagementServiceServer;
use proto::raft_net::raft_service_server::RaftServiceServer;
use proto::shard_net::shard_service_server::ShardServiceServer;
use replication::{
    channel_pool::ChannelPool,
    cluster_management_service::{ClusterManager, InitStatus},
    network::BaseNetwork,
    raft_service::RaftServer,
    shard_service::ShardServer,
    version_store::MemStore, TAIL_NID,
};
use std::{env, sync::Arc};
use tokio::sync::{mpsc, watch, Notify};
use tonic::transport::Server;

#[tokio::main]
async fn main() {
    // tail addr, <Cluster addrs>, my config address, my node id (index into cluster addresses)
    let mut args: Vec<String> = env::args().skip(1).collect();
    assert!(args.len() > 3, "missing command line args");
    let ind = args.pop().unwrap().parse::<usize>().unwrap();
    assert!(ind != TAIL_NID as usize, "tail id must be 0");
    let my_config_addr = args.pop().unwrap().parse().unwrap();
    let my_cluster_addr = args[ind].to_owned().parse().unwrap();
    let cluster_node_ids = (1..u64::try_from(args.len() - 1).unwrap()).collect();

    // start cluster configuration server
    let (handle, mut rx) = mpsc::channel(10);

    tokio::spawn(async move {
        let cluster_manager = ClusterManagementServiceServer::new(ClusterManager::new(handle));

        if let Err(_) = Server::builder()
            .add_service(cluster_manager)
            .serve(my_config_addr)
            .await
        {
            panic!("Cluster management service failed to initialize")
        }
    });

    let mut conn_pool = ChannelPool::new();
    let msg = rx.recv().await.unwrap();
    if let InitStatus::Connect = msg {
        // set up connections
        conn_pool.init(args);
    } else {
        panic!("Node not yet connected")
    }

    //init data structures
    let node_id = u64::try_from(ind).unwrap();
    let conn_pool = Arc::new(conn_pool);
    let config = Arc::new(Config::default().validate().unwrap());
    let (write_notif, ssn_watch) = watch::channel(0);
    let (read_notif, sh_exec_watch) = watch::channel(0);
    let store = Arc::new(MemStore::new(write_notif, read_notif));
    let network = BaseNetwork::new(conn_pool.clone());

    // init raft
    let raft = Arc::new(Raft::new(node_id, config, Arc::new(network), store.clone()));

    loop {
        if let InitStatus::Init = rx.recv().await.unwrap() {
            let notif = Arc::new(Notify::new());
            let notif1 = notif.clone();
            tokio::spawn({
                let raft = raft.clone();
                async move {
                    let raft_server = RaftServiceServer::new(RaftServer::new(raft.clone()));
                    let shard_server = ShardServiceServer::new(ShardServer::new(
                        raft.clone(),
                        conn_pool.clone(),
                        store.clone(),
                        node_id,
                        ssn_watch,
                        sh_exec_watch,
                    ));
                    if let Err(_) = Server::builder()
                        .add_service(shard_server)
                        .add_service(raft_server)
                        .serve(my_cluster_addr)
                        .await
                    {
                        panic!("Raft and shard service intialization failure")
                    }
                    notif1.notify_one();
                }
            });

            // init cluster
            notif.notified().await;
            if let Err(e) = raft.initialize(cluster_node_ids).await {
                panic!("Initialization error {}", e)
            };
            return;
        }
    }
}

// TODO: Tests
