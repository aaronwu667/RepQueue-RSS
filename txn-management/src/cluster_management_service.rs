use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use proto::{
    cluster_management_net::{cluster_management_service_server::ClusterManagementService, NodeId},
    common_decls::Empty,
    manager_net::manager_service_server::ManagerServiceServer,
};
use replication::channel_pool::ChannelPool;
use tokio::sync::{Mutex, RwLock};
use tonic::{transport::Server, Request, Response, Status};

use crate::{transaction_service::TransactionService, ManagerNodeState, NodeStatus};

pub struct ClusterManager {
    node_status: Arc<NodeStatus>,
    conn_pool: Arc<ChannelPool<u32>>,
}

impl ClusterManager {
    pub fn new(
        node_status: NodeStatus,
        my_cluster_addr: std::net::SocketAddr,
        num_shards: u32,
        conn_pool: ChannelPool<u32>,
    ) -> Self {
        // init data structures
        let mut ssn_map = HashMap::new();
        for i in 0..num_shards {
            ssn_map.insert(i, 1);
        }
        let ongoing_txs = RwLock::new(HashMap::new());
        let txn_queues = RwLock::new(HashMap::new());
        let ind_to_sh = Mutex::new(HashMap::new());
        let ssn_map = Mutex::new(ssn_map);
        let read_meta = Mutex::new(HashMap::new());
        let manager_node_state = ManagerNodeState {
            ongoing_txs,
            txn_queues,
            ind_to_sh,
            ssn_map,
            num_shards,
            read_meta,
        };
        let manager_node_state = Arc::new(manager_node_state);

        // network related data
        let node_status = Arc::new(node_status);
        let conn_pool = Arc::new(conn_pool);

        tokio::spawn({
            let node_status = node_status.clone();
            let conn_pool = conn_pool.clone();
            async move {
                let txn_service = ManagerServiceServer::new(TransactionService::new(
                    manager_node_state,
                    conn_pool,
                    node_status,
                ));
                if let Err(_) = Server::builder()
                    .add_service(txn_service)
                    .serve(my_cluster_addr)
                    .await
                {
                    panic!("Transaction manager service failure")
                }
            }
        });
        Self {
            node_status,
            conn_pool,
        }
    }
}

#[async_trait]
impl ClusterManagementService for ClusterManager {
    async fn connect_node(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        let node_connect_res = self.node_status.connect().await;
        let pool_connect_res = self.conn_pool.connect().await;
        match (node_connect_res, pool_connect_res) {
            (Err(s), Err(r)) => Err(Status::internal(format!(
                "Both pool and chain connections failed. Pool {} \n Chain {}",
                s, r
            ))),
            (Err(s), Ok(())) => Err(Status::internal(format!("Chain connection failed {}", s))),
            (Ok(()), Err(s)) => Err(Status::internal(format!("Cluster connection failed {}", s))),
            _ => Ok(Response::new(Empty {})),
        }
    }

    async fn start_cluster(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        return Ok(Response::new(Empty {}));
    }

    async fn add_member(&self, _: Request<NodeId>) -> Result<Response<Empty>, Status> {
        return Ok(Response::new(Empty {}));
    }

    async fn init_leader(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        return Ok(Response::new(Empty {}));
    }
}
