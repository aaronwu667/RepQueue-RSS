use std::sync::Arc;

use crate::channel_pool::ChannelPool;
use crate::StoreRequest;
use anyhow::Result;
use openraft::{
    async_trait::async_trait, types::v070::InstallSnapshotRequest,
    types::v070::InstallSnapshotResponse, types::v070::VoteRequest, types::v070::VoteResponse,
    AppendEntriesRequest, AppendEntriesResponse, LogId, NodeId, RaftNetwork,
};
use proto::raft_net::raft_service_client::RaftServiceClient;

pub struct BaseNetwork {
    connections: Arc<ChannelPool<NodeId>>,
}

impl BaseNetwork {
    pub fn new(conf: Arc<ChannelPool<NodeId>>) -> Self {
        Self {
            connections: conf,
        }
    }
}

// sending rpcs
#[async_trait]
impl RaftNetwork<StoreRequest> for BaseNetwork {
    async fn send_append_entries(
        &self,
        target: NodeId,
        rpc: AppendEntriesRequest<StoreRequest>,
    ) -> Result<AppendEntriesResponse> {
        // get client from channel pool
        let mut client = self
            .connections
            .get_client(|c| RaftServiceClient::new(c), target)
            .await;

        let resp = client.append_entries(rpc).await?.into_inner();
        let resp = AppendEntriesResponse {
            term: resp.term,
            success: resp.success,
            conflict: resp.conflict,
        };
        Ok(resp)
    }
    async fn send_install_snapshot(
        &self,
        target: NodeId,
        rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        let mut client = self
            .connections
            .get_client(|c| RaftServiceClient::new(c), target)
            .await;

        let resp = client.install_snapshot(rpc).await?.into_inner();
        let resp = InstallSnapshotResponse {
            term: resp.term,
            last_applied: resp.last_applied.map(|l| LogId::from(l)),
        };
        Ok(resp)
    }
    async fn send_vote(&self, target: NodeId, rpc: VoteRequest) -> Result<VoteResponse> {
        let mut client = self
            .connections
            .get_client(|c| RaftServiceClient::new(c), target)
            .await;

        let resp = client.request_vote(rpc).await?.into_inner();
        let resp = VoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
            last_log_id: resp.last_log_id.map(|l| LogId::from(l)),
        };
        Ok(resp)
    }
}
