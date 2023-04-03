use proto::{
    client_lib::{client_library_client::ClientLibraryClient, SessionRespWriteRequest},
    manager_net::{AppendTransactRequest, ExecAppendTransactRequest},
    shard_net::{shard_service_client::ShardServiceClient, ExecAppendRequest, ExecReadRequest},
};
use replication::channel_pool::ChannelPool;
use std::sync::Arc;

use crate::Connection;

use crate::DEBUG;

// boilerplate for sending RPCs
#[derive(Debug)]
pub(super) enum RPCRequest {
    AppendTransact(AppendTransactRequest),
    ExecAppendTransact(ExecAppendTransactRequest),
    ExecAppend(ExecAppendRequest),
    ExecRead(ExecReadRequest),
    SessResponseWrite(SessionRespWriteRequest),
}

pub(super) async fn send_client_rpc(
    req: RPCRequest,
    cid: u64,
    addr: String,
    client_conns: Arc<ChannelPool<u64>>,
) {
    if DEBUG {
        println!("Sending {:?} to addr {}", req, addr);
    }
    match req {
        RPCRequest::SessResponseWrite(req) => {
            let mut c = client_conns
                .get_or_connect(ClientLibraryClient::new, cid, addr)
                .await;
            if let Err(e) = c.session_resp_write(req).await {
                eprintln!("Sending to client failed {}", e);
            }
        }
        _ => eprintln!("Wrong type of request for client"),
    }
}

pub(super) async fn send_chain_rpc(req: RPCRequest, conn: Arc<Connection>) {
    let mut client = conn.get_client().await;
    if DEBUG {
        println!("{:?}", req)
    }
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

// TODO (low priority): Handle wrong leader response
pub(super) async fn send_cluster_rpc(sid: u32, req: RPCRequest, pool: Arc<ChannelPool<u32>>) {
    if DEBUG {
        println!("{:?}", req)
    }
    let mut client = pool.get_client(ShardServiceClient::new, sid).await;
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
