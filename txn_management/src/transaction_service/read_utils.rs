use super::sharding::get_buckets;
use super::{ManagerNodeState, NotifyFuture, NotifyFutureWrap, TxnStatus};
use fasthash::xx;
use futures::FutureExt;
use std::collections::{btree_map, BTreeMap};
use std::ops::Bound::*;
use std::{cmp::max, collections::HashMap};
use tokio::sync::oneshot;

// returns read fence and subtransactions
pub(super) async fn get_queue_fence(
    state: &ManagerNodeState,
    read_set: Vec<String>,
) -> (u64, HashMap<u32, Vec<String>>) {
    let mut fence = 0;
    let mut res = HashMap::<u32, Vec<String>>::new();
    let buckets = get_buckets(state.num_shards);
    let txn_queues = state.txn_queues.read().await;
    for key in read_set {
        // hash key
        let hash = xx::hash64(key.as_bytes());
        let mut after = buckets.range((Excluded(hash), Unbounded));
        let (_, ub) = match after.next() {
            Some(ub) => ub,
            None => panic!("Greatest element of tree should be u64 MAX"),
        };
        res.entry(ub.sid)
            .and_modify(|req| req.push(key.clone()))
            .or_insert(Vec::from([key]));
        fence = match txn_queues.get(&ub.sid) {
            Some((last_exec, _)) => max(fence, *last_exec),
            None => fence,
        };
    }
    (fence, res)
}

// If already a future, return, else create new one and add handle to map
pub(super) fn register_future(
    write_dep: u64,
    csn_map: &mut BTreeMap<u64, TxnStatus>,
) -> Option<NotifyFuture<bool>> {
    match csn_map.entry(write_dep) {
        btree_map::Entry::Occupied(txn_ent) => {
            if let TxnStatus::NotStarted(fut, _) = &txn_ent.get() {
                Some(fut.clone())
            } else {
                None
            }
        }
        btree_map::Entry::Vacant(v_txn) => {
            let (sender, recv) = oneshot::channel::<bool>();
            let fut = NotifyFutureWrap(recv).shared();
            v_txn.insert(TxnStatus::NotStarted(fut.clone(), sender));
            Some(fut)
        }
    }
}
