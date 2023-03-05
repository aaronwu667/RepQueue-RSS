use async_trait::async_trait;
use proto::{
    cluster_management_net::cluster_management_service_server::ClusterManagementService,
    common_decls::Empty,
};
use tokio::sync::mpsc::Sender;
use tonic::{Request, Response, Status};

pub enum InitStatus {
    Connect,
    Init,
}

pub struct ClusterManager {
    handle: Sender<InitStatus>,
}

impl ClusterManager {
    pub fn new(handle: Sender<InitStatus>) -> Self {
        ClusterManager { handle }
    }
}

#[async_trait]
impl ClusterManagementService for ClusterManager {
    async fn connect_node(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        if let Err(_) = self.handle.send(InitStatus::Connect).await {
            panic!("sending on cluster management handle failed, cluster may already be running");
        }
        return Ok(Response::new(Empty {}));
    }

    async fn init_cluster(&self, _: Request<Empty>) -> Result<Response<Empty>, Status> {
        if let Err(_) = self.handle.send(InitStatus::Init).await {
            panic!("sending on cluster management handle failed, cluster may already be running");
        }
        return Ok(Response::new(Empty {}));
    }
}
