use crate::{transaction_service::TransactionService, NodeStatus};
use async_trait::async_trait;
use proto::{
    chain_management_net::{
        chain_management_service_server::ChainManagementService, InitNodeRequest,
    },
    common_decls::Empty,
    manager_net::manager_service_server::ManagerServiceServer,
};
use replication::channel_pool::ChannelPool;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

pub struct ChainManager {
    node_status: Mutex<Option<Arc<NodeStatus>>>,
    conn_pool: Arc<ChannelPool<u32>>,
    my_cluster_addr: std::net::SocketAddr,
}

impl ChainManager {
    pub fn new(my_cluster_addr: String) -> Self {
        let conn_pool = Arc::new(ChannelPool::new());
        Self {
            node_status: Mutex::new(None),
            conn_pool,
            my_cluster_addr: my_cluster_addr.parse().unwrap(),
        }
    }
}

#[async_trait]
impl ChainManagementService for ChainManager {
    async fn init_node(
        &self,
        request: Request<InitNodeRequest>,
    ) -> Result<Response<Empty>, Status> {
        let mut node_status = self.node_status.lock().await;
        if node_status.is_some() {
            return Err(Status::already_exists("Node already initialized"));
        }

        let req = request.into_inner();

        // add addresses to pool
        let conn_pool = self.conn_pool.clone();
        let my_cluster_addr = self.my_cluster_addr;
        let num_shards = u32::try_from(req.leader_addrs.len()).unwrap();
        conn_pool.add_addrs(None, req.leader_addrs).await;

        // construct node status
        let status = match req.node_type {
            0 => match req.succ {
                Some(addr) => NodeStatus::new_head(addr.parse().unwrap()),
                None => return Err(Status::invalid_argument("Missing successor address")),
            },
            1 => match (req.pred, req.succ) {
                (Some(pred_addr), Some(succ_addr)) => {
                    NodeStatus::new_middle(pred_addr.parse().unwrap(), succ_addr.parse().unwrap())
                }
                _ => {
                    return Err(Status::invalid_argument(
                        "Missing either predecessor or successor address",
                    ))
                }
            },
            2 => match req.pred {
                Some(addr) => NodeStatus::new_tail(addr.parse().unwrap()),
                None => return Err(Status::invalid_argument("Missing predecessor address")),
            },
            _ => return Err(Status::invalid_argument("Invalid node type")),
        };
        let arc_status = Arc::new(status);
        *node_status = Some(arc_status.clone());

        tokio::spawn({
            async move {
                let txn_service = ManagerServiceServer::new(TransactionService::new(
                    num_shards, conn_pool, arc_status,
                ));
                if Server::builder()
                    .add_service(txn_service)
                    .serve(my_cluster_addr)
                    .await
                    .is_err()
                {
                    panic!("Transaction manager service failure")
                }
            }
        });

        Ok(Response::new(Empty {}))
    }

    async fn connect_node(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        let node_status = self.node_status.lock().await;
        let node_status = match &*node_status {
            Some(s) => s,
            None => return Err(Status::not_found("Node not yet initialized")),
        };
        let node_connect_res = node_status.connect().await;
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
    async fn get_metrics(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        let node_status = self.node_status.lock().await;
        let _ = match &*node_status {
            Some(s) => s,
            None => return Err(Status::not_found("Node not yet initialized")),
        };
        return Ok(Response::new(Empty {}));
    }
}
