use std::sync::Arc;

use proto::{manager_net::{manager_service_client::ManagerServiceClient, AppendTransactRequest, ExecAppendTransactRequest}, shard_net::{ExecAppendRequest, shard_service_client::ShardServiceClient}};
use replication::channel_pool::ChannelPool;
use tonic::{transport::Channel, Request};

// boilerplate for sending RPCs
pub(crate) enum RPCRequest {
    Append(AppendTransactRequest),
    ExecAppend(ExecAppendTransactRequest),
}

pub(crate) async fn send_chain_rpc(req: RPCRequest, ch: Channel) {
    let mut client = ManagerServiceClient::new(ch.clone());
    match req {
        RPCRequest::Append(req) => {
            if let Err(e) = client.append_transact(Request::new(req)).await {
                eprintln!("Chain replication failed: {}", e);
            }
        }
        RPCRequest::ExecAppend(req) => {
            if let Err(e) = client.exec_append_transact(Request::new(req)).await {
                eprintln!("Backwards ack failed: {}", e);
            }
        }
    }
}

pub(crate) async fn send_exec(sid: u32, req: ExecAppendRequest, pool: Arc<ChannelPool<u32>>) {
    let mut client = pool
        .get_client(|c| ShardServiceClient::new(c.clone()), sid)
        .await;
    if let Err(e) = client.shard_exec_append(Request::new(req)).await {
        eprintln!("communication with cluster failed: {}", e);
    }
}
