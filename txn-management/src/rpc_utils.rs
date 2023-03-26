use proto::{
    client_lib::{client_library_client::ClientLibraryClient, SessionRespWriteRequest},
    manager_net::{AppendTransactRequest, ExecAppendTransactRequest},
    shard_net::{shard_service_client::ShardServiceClient, ExecAppendRequest, ExecReadRequest},
};
use replication::channel_pool::ChannelPool;
use std::sync::Arc;

use crate::Connection;

// boilerplate for sending RPCs
pub(crate) enum RPCRequest {
    AppendTransact(AppendTransactRequest),
    ExecAppendTransact(ExecAppendTransactRequest),
    ExecAppend(ExecAppendRequest),
    ExecRead(ExecReadRequest),
    SessResponseWrite(SessionRespWriteRequest),
}

pub(crate) async fn send_client_rpc(req: RPCRequest, addr: String) {
    match req {
        RPCRequest::SessResponseWrite(req) => match ClientLibraryClient::connect(addr).await {
            Ok(mut c) => {
                if let Err(e) = c.session_resp_write(req).await {
                    eprintln!("Sending to client failed {}", e);
                }
            }
            Err(e) => eprintln!("Connecting to client failed {}", e),
        },
        _ => eprintln!("Wrong type of request for client"),
    }
}

pub(crate) async fn send_chain_rpc(req: RPCRequest, conn: Arc<Connection>) {
    let mut client = conn.get_client().await;
    match req {
        RPCRequest::AppendTransact(req) => {
            if let Err(e) = client.append_transact(req).await {
                eprintln!("Chain replication failed: {}", e);
            }
        }
        RPCRequest::ExecAppendTransact(req) => {
            if let Err(e) = client.exec_append_transact(req).await {
                eprintln!("Backwards ack failed: {}", e);
            }
        }
        _ => eprintln!("Wrong type of request for chain"),
    }
}

pub(crate) async fn send_cluster_rpc(sid: u32, req: RPCRequest, pool: Arc<ChannelPool<u32>>) {
    let mut client = pool
        .get_client(|c| ShardServiceClient::new(c.clone()), sid)
        .await;
    match req {
        RPCRequest::ExecAppend(req) => {
            if let Err(e) = client.shard_exec_append(req).await {
                eprintln!("communication with cluster failed: {}", e);
            }
        }
        RPCRequest::ExecRead(req) => {
            if let Err(e) = client.shard_exec_read(req).await {
                eprintln!("communication with cluster failed: {}", e);
            }
        }
        _ => eprintln!("Wrong type of request for cluster"),
    }
}
