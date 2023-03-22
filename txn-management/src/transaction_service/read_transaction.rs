use crate::rpc_utils::send_cluster_rpc;
use crate::rpc_utils::RPCRequest;
use crate::ManagerNodeState;
use crate::TxnStatus;
use proto::manager_net::ReadOnlyTransactRequest;
use proto::shard_net::ExecReadRequest;
use replication::channel_pool::ChannelPool;
use std::cmp::max;
use std::collections::btree_map;
use std::collections::hash_map;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Notify;

use super::read_utils::get_queue_fence;
use super::TransactionService;

// Read only transactions
// TODO (high priority): Support client sessions
impl TransactionService {
    pub(super) async fn proc_read(
        request: ReadOnlyTransactRequest,
        state: Arc<ManagerNodeState>,
        cluster_conns: Arc<ChannelPool<u32>>,
    ) {
        let csn = request.csn.unwrap();
        // if the dependency is not present, wait on a notify
        let mut ongoing_txns = state.ongoing_txs.write().await;
        match ongoing_txns.entry(csn.cid) {
            hash_map::Entry::Occupied(mut csn_map) => match csn_map.get_mut().entry(csn.sn) {
                btree_map::Entry::Occupied(txn_ent) => {
                    if let TxnStatus::NotStarted(notif) = &txn_ent.get() {
                        notif.notified().await;
                    }
                }
                btree_map::Entry::Vacant(v_txn) => {
                    let wait = Arc::new(Notify::new());
                    v_txn.insert(TxnStatus::NotStarted(wait.clone()));
                    wait.notified().await;
                }
            },
            hash_map::Entry::Vacant(v) => {
                let mut new_client_map = BTreeMap::new();
                let wait = Arc::new(Notify::new());
                new_client_map.insert(csn.sn, TxnStatus::NotStarted(wait.clone()));
                v.insert(new_client_map);
                wait.notified().await;
            }
        }
        drop(ongoing_txns);

        // Compute read fence and update client read metadata
        let (mut fence, sub_txns) = get_queue_fence(&*state, request.keys).await;
        let mut read_meta = state.read_meta.lock().await;
        if request.is_retry {
            fence = request.lsn_const;
            match read_meta.entry(csn.cid) {
                hash_map::Entry::Occupied(mut o) => {
                    if csn.sn > o.get().0 {
                        o.insert((csn.sn, fence));
                    }
                }
                hash_map::Entry::Vacant(v) => {
                    v.insert((csn.sn, fence));
                }
            }
        } else {
            match read_meta.entry(csn.cid) {
                hash_map::Entry::Occupied(mut o) => {
                    if csn.sn <= o.get().0 {
                        fence = o.get().1;
                    } else {
                        fence = max(fence, o.get().1);
                        o.insert((csn.sn, fence));
                    }
                }
                hash_map::Entry::Vacant(v) => {
                    v.insert((csn.sn, fence));
                }
            }
        }
        // send to cluster
        let num_shards = sub_txns.len();
        for (k, v) in sub_txns.into_iter() {
            let req = ExecReadRequest {
                txn: v,
                csn: Some(csn.clone()),
                fence,
                num_shards: u32::try_from(num_shards).unwrap(),
                addr: "".to_owned(),
            };
            tokio::spawn(send_cluster_rpc(
                k,
                RPCRequest::ExecRead(req),
                cluster_conns.clone(),
            ));
        }
    }
}
