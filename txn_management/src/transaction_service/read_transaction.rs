use super::read_utils::get_watch;
use super::rpc_utils::{send_cluster_rpc, RPCRequest};
use proto::common_decls::Csn;
use proto::manager_net::ReadOnlyTransactRequest;
use proto::shard_net::ExecReadRequest;
use replication::channel_pool::ChannelPool;
use std::cmp::max;
use std::collections::hash_map;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;

use super::read_utils::get_queue_fence;
use super::ManagerNodeState;
use super::TransactionService;
use super::TxnStatus;

// Read only transactions
// TODO (high priority): Support client sessions
impl TransactionService {
    pub(super) async fn proc_read(
        register_dep_tx: mpsc::Sender<(Csn, watch::Sender<bool>)>,
        request: ReadOnlyTransactRequest,
        state: Arc<ManagerNodeState>,
        cluster_conns: Arc<ChannelPool<u32>>,
    ) {
        let csn = request.csn.unwrap();
        if let Some(write_dep) = request.write_dep {
            let mut ongoing_txns = state.ongoing_txs.write().await;
            let watch = match ongoing_txns.entry(csn.cid) {
                hash_map::Entry::Occupied(mut csn_map) => {
                    let csn_map = csn_map.get_mut();
                    if let Some((max_in_prog, _)) = csn_map.last_key_value() {
                        if write_dep > *max_in_prog {
                            get_watch(write_dep, csn_map)
                        } else {
                            (None, None)
                        }
                    } else {
                        get_watch(write_dep, csn_map)
                    }
                }
                hash_map::Entry::Vacant(v) => {
                    let mut new_client_map = BTreeMap::new();
                    let (sender, recv) = watch::channel(false);
                    new_client_map.insert(csn.sn, TxnStatus::NotStarted(recv.clone()));
                    v.insert(new_client_map);
                    (Some(sender), Some(recv))
                }
            };
            drop(ongoing_txns);

            match watch {
                (Some(s), Some(mut r)) => {
                    if let Err(_) = register_dep_tx.send((csn.clone(), s)).await {
                        panic!("Register dep receiver dropped")
                    }
                    if let Err(_) = r.changed().await {}
                }
                (None, Some(mut r)) => if let Err(_) = r.changed().await {},
                _ => (),
            }
        }

        // Compute read fence and update client read metadata
        let (mut fence, sub_txns) = get_queue_fence(&*state, request.keys).await;
        let mut read_meta = state.read_meta.lock().await;
        if let Some(constraint) = request.lsn_const {
            fence = constraint;
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
                addr: request.addr.clone(),
            };
            tokio::spawn(send_cluster_rpc(
                k,
                RPCRequest::ExecRead(req),
                cluster_conns.clone(),
            ));
        }
    }
}
