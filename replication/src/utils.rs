use crate::channel_pool::ChannelPool;
use crate::version_store::MemStore;
use crate::StoreRequest;
use crate::{Op::*, StoreResponse, DEBUG, TAIL_NID};
use openraft::NodeId;
use proto::client_lib::client_library_client::ClientLibraryClient;
use proto::client_lib::SessionRespReadRequest;
use proto::common_decls::transaction_op::Op::*;
use proto::common_decls::{ExecNotifRequest, TxnRes, ValueField};
use proto::manager_net::manager_service_client::ManagerServiceClient;
use proto::shard_net::shard_service_client::ShardServiceClient;
use proto::shard_net::{ExecAppendRequest, ExecReadRequest, PutReadRequest};
use std::collections::HashMap;
use std::sync::Arc;

// RPC utils
pub(super) async fn serve_remote(
    remote_id: u64,
    ind: u64,
    vals: HashMap<String, ValueField>,
    connections: Arc<ChannelPool<NodeId>>,
) {
    let mut client = connections
        .get_client(ShardServiceClient::new, remote_id)
        .await;
    if let Err(e) = client.put_read(PutReadRequest { ind, res: vals }).await {
        eprintln!("Serving remote read failed: {}", e);
    }
}

pub(super) async fn serve_tail(resp: ExecNotifRequest, connections: Arc<ChannelPool<NodeId>>) {
    tokio::spawn({
        let conns = connections;
        async move {
            let mut client = conns.get_client(ManagerServiceClient::new, TAIL_NID).await;
            if let Err(e) = client.exec_notif(resp).await {
                eprintln!("Error when sending to manager node {}", e);
            }
        }
    });
}

pub(super) async fn serve_read(
    ent: ExecReadRequest,
    store: Arc<MemStore>,
    client_conns: Arc<ChannelPool<u64>>,
) {
    let state = store.state.read().await;
    let mut res_map = HashMap::<String, ValueField>::new();
    for read_key in ent.txn.into_iter() {
        let read_res = state.get(&read_key, &ent.fence);
        res_map.insert(read_key, ValueField::new(read_res));
    }
    let csn = ent.csn.unwrap();
    let cid = csn.cid;
    let resp = SessionRespReadRequest::new(res_map, Some(csn), ent.fence, ent.num_shards);
    tokio::spawn(async move {
        let mut client = client_conns
            .get_or_connect(ClientLibraryClient::new, cid, ent.addr)
            .await;
        if DEBUG {
            println!("Sending response to client library {:?}", resp);
        }
        if let Err(e) = client.session_resp_read(resp).await {
            eprintln!("Error when sending to client: {}", e);
        }
    });
}

// Miscellaneous data mapping utils
pub(crate) fn add_remote_deps(
    k: String,
    v: Option<String>,
    map: &mut HashMap<u64, HashMap<String, ValueField>>,
    remote_groups: Vec<u64>,
) {
    for group in remote_groups {
        map.entry(group)
            .and_modify(|m| {
                m.insert(k.to_owned(), ValueField::new(v.to_owned()));
            })
            .or_insert_with(|| {
                let mut new_map = HashMap::new();
                new_map.insert(k.to_owned(), ValueField::new(v.to_owned()));
                new_map
            });
    }
}

impl From<&ExecAppendRequest> for StoreRequest {
    fn from(value: &ExecAppendRequest) -> Self {
        // in the future, condition parsing/eval goes here
        StoreRequest {
            subtxn: value
                .txn
                .iter()
                .map(|(k, v)| {
                    let op = match &v.op {
                        Some(Read(_)) => Get,
                        Some(Write(write_val)) => Put(write_val.to_owned()),
                        Some(CondWrite(t)) => crate::Op::Put(t.val.to_owned()),
                        None => panic!("Operation empty!"),
                    };
                    (k.to_owned(), op)
                })
                .collect(),
            ind: value.ind,
            ssn: value.sn,
        }
    }
}

impl From<StoreResponse> for Option<TxnRes> {
    fn from(value: StoreResponse) -> Self {
        value.res.map(|m| {
            let mut map = HashMap::new();
            for (k, v) in m.into_iter() {
                map.insert(k, ValueField::new(v));
            }
            TxnRes { map }
        })
    }
}
