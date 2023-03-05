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
    version_store::MemStore,
};
use std::{env, sync::Arc};
use tokio::{
    runtime::Runtime,
    sync::{mpsc, watch, Notify},
};
use tonic::transport::Server;

fn main() {
    // <Cluster addresses>, my config address, my node id (index into cluster addresses)
    let mut args: Vec<String> = env::args().collect();
    assert!(args.len() > 2, "Must specify nodeId and address");
    let ind = args.pop().unwrap().parse::<usize>().unwrap();
    let my_config_addr = args.pop().unwrap().parse().unwrap();
    let my_cluster_addr = args[ind].to_owned().parse().unwrap();
    let cluster_node_ids = (0..u64::try_from(args.len() - 1).unwrap()).collect();

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

    let msg = rx.blocking_recv().unwrap();
    if let InitStatus::Init = msg {
        panic!("Node not yet connected")
    }

    // set up connections
    let mut conn_pool = ChannelPool::new();
    conn_pool.init(args);

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
        if let InitStatus::Init = rx.blocking_recv().unwrap() {
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
            Runtime::new().unwrap().block_on(notif.notified());
            if let Err(e) = Runtime::new()
                .unwrap()
                .block_on(raft.initialize(cluster_node_ids))
            {
                panic!("Initialization error {}", e)
            };
            return;
        }
    }
}
