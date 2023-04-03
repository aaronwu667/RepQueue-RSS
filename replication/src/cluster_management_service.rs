use std::{collections::BTreeSet, sync::Arc};

use async_trait::async_trait;
use openraft::{Config, Raft};
use proto::{
    cluster_management_net::{cluster_management_service_server::ClusterManagementService, NodeId},
    common_decls::Empty,
    raft_net::raft_service_server::RaftServiceServer,
    shard_net::shard_service_server::ShardServiceServer,
};
use tokio::sync::watch;
use tonic::{transport::Server, Request, Response, Status};

use crate::{
    channel_pool::ChannelPool, network::BaseNetwork, raft_service::RaftServer,
    shard_service::ShardServer, version_store::MemStore, StoreRequest, StoreResponse,
};

pub struct ClusterManager {
    node_id: u64,
    raft: Arc<Raft<StoreRequest, StoreResponse, BaseNetwork, MemStore>>,
    conn_pool: Arc<ChannelPool<u64>>,
}

// TODO(low priority): Metrics and monitoring
impl ClusterManager {
    pub fn new(
        shard_id: u32,
        node_id: u64,
        my_cluster_addr: std::net::SocketAddr,
        conn_pool: ChannelPool<u64>,
    ) -> Self {
        //tracing_subscriber::fmt::init();
        let conn_pool = Arc::new(conn_pool);

        // init raft
        let config = Arc::new(Config::default().validate().unwrap());
        let (write_notif, ssn_watch) = watch::channel(0);
        let (read_notif, sh_exec_watch) = watch::channel(0);
        let store = Arc::new(MemStore::new(write_notif, read_notif));
        let network = Arc::new(BaseNetwork::new(conn_pool.clone()));
        let raft = Arc::new(Raft::new(node_id, config, network, store.clone()));

        // start servers
        tokio::spawn({
            let raft = raft.clone();
            let conn_pool = conn_pool.clone();
            async move {
                let raft_server = RaftServiceServer::new(RaftServer::new(raft.clone()));
                let shard_server = ShardServiceServer::new(ShardServer::new(
                    raft.clone(),
                    conn_pool.clone(),
                    store.clone(),
                    shard_id,
                    ssn_watch,
                    sh_exec_watch,
                ));
                // as per docs, will panic if binding to addr fails
                println!("Shard and raft servers running on {}", my_cluster_addr);
                if Server::builder()
                    .add_service(shard_server)
                    .add_service(raft_server)
                    .serve(my_cluster_addr)
                    .await
                    .is_err()
                {
                    panic!("Raft and shard service failure")
                }
            }
        });

        Self {
            node_id,
            raft,
            conn_pool,
        }
    }
}

#[async_trait]
impl ClusterManagementService for ClusterManager {
    async fn connect_node(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        match self.conn_pool.connect().await {
            Ok(()) => return Ok(Response::new(Empty {})),
            Err(s) => return Err(Status::internal(s)),
        }
    }

    async fn start_cluster(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        if self.node_id == 1 {
            let mut nids: BTreeSet<u64> = self.conn_pool.get_all_nodes().await;
            nids.remove(&0);
            nids.insert(1);
            match self.raft.change_membership(nids, true).await {
                Ok(_) => return Ok(Response::new(Empty {})),
                Err(_) => return Err(Status::internal("Cluster init error")),
            }
        } else {
            return Err(Status::invalid_argument("Node is not leader"));
        }
    }

    async fn add_member(&self, req: Request<NodeId>) -> Result<Response<Empty>, Status> {
        if self.node_id == 1 {
            match self.raft.add_learner(req.into_inner().id, true).await {
                Ok(_) => return Ok(Response::new(Empty {})),
                Err(_) => return Err(Status::internal("Learner addition error")),
            }
        } else {
            return Err(Status::invalid_argument("Node is not leader"));
        }
    }

    async fn init_leader(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        if self.node_id == 1 {
            match self.raft.initialize(BTreeSet::from([1])).await {
                Ok(_) => return Ok(Response::new(Empty {})),
                Err(_) => return Err(Status::internal("Initialization error")),
            }
        } else {
            return Err(Status::invalid_argument("Node is not leader"));
        }
    }

    async fn get_metrics(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        let metrics = self.raft.metrics().borrow().clone();
        println!("{:?}", metrics);
        Ok(Response::new(Empty {}))
    }
}
