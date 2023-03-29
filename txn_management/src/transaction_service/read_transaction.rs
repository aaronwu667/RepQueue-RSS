use super::read_utils::register_future;
use super::rpc_utils::{send_cluster_rpc, RPCRequest};
use futures::FutureExt;
use proto::manager_net::ReadOnlyTransactRequest;
use proto::shard_net::ExecReadRequest;
use replication::channel_pool::ChannelPool;
use std::cmp::max;
use std::collections::hash_map;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::oneshot;

use super::read_utils::get_queue_fence;
use super::ManagerNodeState;
use super::TxnStatus;
use super::{NotifyFutureWrap, TransactionService};

// Read only transactions
// TODO (high priority): Support client sessions
impl TransactionService {
    pub(super) async fn proc_read(
        request: ReadOnlyTransactRequest,
        state: Arc<ManagerNodeState>,
        cluster_conns: Arc<ChannelPool<u32>>,
    ) {
        let csn = request.csn.expect("Csn should not be empty");
        if let Some(write_dep) = request.write_dep {
            let mut ongoing_txns = state.ongoing_txs.lock().await;
            let watch = match ongoing_txns.entry(csn.cid) {
                hash_map::Entry::Occupied(mut csn_map) => {
                    let csn_map = csn_map.get_mut();
                    if let Some((max_in_prog, _)) = csn_map.last_key_value() {
                        if write_dep > *max_in_prog {
                            register_future(write_dep, csn_map)
                        } else {
                            None
                        }
                    } else {
                        register_future(write_dep, csn_map)
                    }
                }
                hash_map::Entry::Vacant(v) => {
                    let mut new_client_map = BTreeMap::new();
                    let (sender, recv) = oneshot::channel();
                    let fut = NotifyFutureWrap(recv).shared();
                    new_client_map.insert(write_dep, TxnStatus::NotStarted(fut.clone(), sender));
                    v.insert(new_client_map);
                    Some(fut)
                }
            };
            drop(ongoing_txns);

            // wait for dependency resolution
            if let Some(fut) = watch {
                fut.await;
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
