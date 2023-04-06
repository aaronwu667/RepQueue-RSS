use crate::RaftRepl;
use async_trait::async_trait;
use openraft::raft::{InstallSnapshotRequest, VoteRequest};
use openraft::AppendEntriesRequest;
use proto::raft_net::{
    raft_service_server::RaftService, AppendEntriesRequestProto, AppendEntriesResponseProto,
    InstallSnapshotRequestProto, InstallSnapshotResponseProto, VoteRequestProto, VoteResponseProto,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

// Raft based shard group service
pub struct RaftServer {
    raft: Arc<RaftRepl>,
}

impl RaftServer {
    pub fn new(r: Arc<RaftRepl>) -> Self {
        Self { raft: r }
    }
}

#[async_trait]
impl RaftService for RaftServer {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequestProto>,
    ) -> Result<Response<AppendEntriesResponseProto>, Status> {
        let req = AppendEntriesRequest::from(request.into_inner());
        let res = self.raft.append_entries(req).await;
        Ok(Response::new(AppendEntriesResponseProto::from(
            res.unwrap(),
        )))
    }
    async fn install_snapshot(
        &self,
        request: Request<InstallSnapshotRequestProto>,
    ) -> Result<Response<InstallSnapshotResponseProto>, Status> {
        let req = InstallSnapshotRequest::from(request.into_inner());
        let res = self.raft.install_snapshot(req).await;
        Ok(Response::new(InstallSnapshotResponseProto::from(
            res.unwrap(),
        )))
    }
    async fn request_vote(
        &self,
        request: Request<VoteRequestProto>,
    ) -> Result<Response<VoteResponseProto>, Status> {
        let req = VoteRequest::from(request.into_inner());
        let res = self.raft.vote(req).await;
        Ok(Response::new(VoteResponseProto::from(res.unwrap())))
    }
}
