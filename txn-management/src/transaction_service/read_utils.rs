use crate::{get_buckets, ManagerNodeState};
use fasthash::xx;
use std::ops::Bound::*;
use std::{cmp::max, collections::HashMap};

// returns
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
    return (fence, res);
}
