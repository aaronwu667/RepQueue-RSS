use crate::{
    channel_pool::ChannelPool, network::BaseNetwork, raft_service::RaftServer,
    shard_service::ShardServer, version_store::MemStore, StoreRequest, StoreResponse,
};
use async_trait::async_trait;
use openraft::{Config, Raft};
use proto::{
    cluster_management_net::{
        cluster_management_service_server::ClusterManagementService, InitNodeRequest, NodeId,
    },
    common_decls::Empty,
    raft_net::raft_service_server::RaftServiceServer,
    shard_net::shard_service_server::ShardServiceServer,
};
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::{watch, Mutex};
use tonic::{transport::Server, Request, Response, Status};

struct RaftState {
    node_id: u64,
    raft: Arc<Raft<StoreRequest, StoreResponse, BaseNetwork, MemStore>>,
}

pub struct ClusterManager {
    raft_state: Mutex<Option<RaftState>>,
    conn_pool: Arc<ChannelPool<u64>>,
    my_cluster_addr: std::net::SocketAddr,
}

// TODO(low priority): Metrics and monitoring
impl ClusterManager {
    pub fn new(my_cluster_addr: String) -> Self {
        let conn_pool = Arc::new(ChannelPool::new());
        let my_cluster_addr = my_cluster_addr.parse().unwrap();
        Self {
            raft_state: Mutex::new(None),
            conn_pool,
            my_cluster_addr,
        }
    }
}

#[async_trait]
impl ClusterManagementService for ClusterManager {
    async fn init_node(
        &self,
        request: Request<InitNodeRequest>,
    ) -> Result<Response<Empty>, Status> {
        // make sure we are not initializing twice
        let mut raft_state = self.raft_state.lock().await;
        if raft_state.is_some() {
            return Err(Status::already_exists("Node already initialized"));
        }

        let req = request.into_inner();
        let conn_pool = self.conn_pool.clone();
        let my_cluster_addr = self.my_cluster_addr;
        // add addresses to pool
        conn_pool.add_addrs(req.cluster_addrs).await;

        // init raft
        let config = Arc::new(Config::default().validate().unwrap());
        let (write_notif, ssn_watch) = watch::channel(0);
        let (read_notif, sh_exec_watch) = watch::channel(0);
        let store = Arc::new(MemStore::new(write_notif, read_notif));
        let network = Arc::new(BaseNetwork::new(conn_pool.clone()));
        let raft = Arc::new(Raft::new(req.node_id, config, network, store.clone()));

        // update raft state
        *raft_state = Some(RaftState {
            node_id: req.node_id,
            raft: raft.clone(),
        });

        // start servers
        tokio::spawn({
            let raft = raft;
            let conn_pool = conn_pool.clone();
            async move {
                let raft_server = RaftServiceServer::new(RaftServer::new(raft.clone()));
                let shard_server = ShardServiceServer::new(ShardServer::new(
                    raft.clone(),
                    conn_pool.clone(),
                    store.clone(),
                    req.shard_id,
                    ssn_watch,
                    sh_exec_watch,
                ));
                // as per docs, will panic if binding to addr fails
                println!(
                    "Shard and raft servers running on {}",
                    my_cluster_addr.clone()
                );
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

        Ok(Response::new(Empty {}))
    }

    async fn connect_node(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        match self.conn_pool.connect().await {
            Ok(()) => return Ok(Response::new(Empty {})),
            Err(s) => return Err(Status::internal(s)),
        }
    }

    async fn start_cluster(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        let raft_state = self.raft_state.lock().await;
        let raft_state = match &*raft_state {
            Some(state) => state,
            None => return Err(Status::not_found("Node not yet initialized")),
        };
        if raft_state.node_id == 1 {
            let mut nids: BTreeSet<u64> = self.conn_pool.get_all_nodes().await;
            nids.remove(&0);
            nids.insert(1);
            match raft_state.raft.change_membership(nids, true).await {
                Ok(_) => return Ok(Response::new(Empty {})),
                Err(_) => return Err(Status::internal("Cluster init error")),
            }
        } else {
            return Err(Status::invalid_argument("Node is not leader"));
        }
    }

    async fn add_member(&self, req: Request<NodeId>) -> Result<Response<Empty>, Status> {
        let raft_state = self.raft_state.lock().await;
        let raft_state = match &*raft_state {
            Some(state) => state,
            None => return Err(Status::not_found("Node not yet initialized")),
        };
        if raft_state.node_id == 1 {
            match raft_state.raft.add_learner(req.into_inner().id, true).await {
                Ok(_) => return Ok(Response::new(Empty {})),
                Err(_) => return Err(Status::internal("Learner addition error")),
            }
        } else {
            return Err(Status::invalid_argument("Node is not leader"));
        }
    }

    async fn init_leader(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        let raft_state = self.raft_state.lock().await;
        let raft_state = match &*raft_state {
            Some(state) => state,
            None => return Err(Status::not_found("Node not yet initialized")),
        };
        if raft_state.node_id == 1 {
            match raft_state.raft.initialize(BTreeSet::from([1])).await {
                Ok(_) => return Ok(Response::new(Empty {})),
                Err(_) => return Err(Status::internal("Initialization error")),
            }
        } else {
            return Err(Status::invalid_argument("Node is not leader"));
        }
    }

    async fn get_metrics(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        let raft_state = self.raft_state.lock().await;
        let raft_state = match &*raft_state {
            Some(state) => state,
            None => return Err(Status::not_found("Node not yet initialized")),
        };
        let metrics = raft_state.raft.metrics().borrow().clone();
        println!("{:?}", metrics);
        Ok(Response::new(Empty {}))
    }
}
