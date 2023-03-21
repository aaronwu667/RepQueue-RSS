use fasthash::xx;
use proto::shard_net::ExecAppendRequest;
use std::collections::hash_map::Entry::*;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Bound::*;

use proto::common_decls::TransactionOp;

use crate::transaction_service::ManagerNodeState;

// utils for transaction service
pub(crate) async fn update_view(
    state: &ManagerNodeState,
    ind: usize,
    cid: u64,
    write_set: &HashMap<String, TransactionOp>,
) {
    let mut ssn_map = state.ssn_map.lock().await;
    let num_shards = u32::try_from(ssn_map.len()).unwrap();
    let ind = u64::try_from(ind).unwrap();

    // compute buckets and put into BTreeSet
    let step = u64::MAX / u64::from(num_shards);
    let mut rem = u64::MAX % u64::from(num_shards);
    let mut prev = 0;
    let mut buckets = BTreeMap::new();
    for i in 1..(num_shards + 1) {
        prev = prev + step;
        if rem > 0 {
            prev += 1;
            rem -= 1;
            buckets.insert(prev, (i, false));
        } else {
            buckets.insert(prev, (i, false));
        }
    }

    let mut ind_to_sh = state.ind_to_sh.lock().await;
    let mut txn_queues = state.txn_queues.write().await;
    for k in write_set.keys() {
        // deterministic hashing
        let hash = xx::hash64(k.as_bytes());
        let mut after = buckets.range_mut((Excluded(hash), Unbounded));
        let (_, ub) = match after.next() {
            Some(ub) => ub,
            None => panic!("Greatest element of tree should be u64 MAX"),
        };
        if !ub.1 {
            // insert into transaction queue
            txn_queues
                .entry(ub.0)
                .and_modify(|ent| {
                    ent.1.push_back(ind);
                })
                .or_insert((0, VecDeque::from([ind])));

            // increment shard sequence number
            let ssn = ssn_map.get_mut(&ub.0).unwrap();
            *ssn += 1;

            // update ind to shard mapping
            ind_to_sh
                .entry(ind)
                .and_modify(|ent| {
                    ent.1.insert(ub.0);
                })
                .or_insert((cid, HashSet::from([ub.0])));

            // mark shard as visited
            *ub = (ub.0, true);
        }
    }
}

// TODO(med priority): dependency analysis in loop
pub(crate) async fn update_view_tail(
    state: &ManagerNodeState,
    ind: usize,
    cid: u64,
    write_set: HashMap<String, TransactionOp>,
) -> HashMap<u32, ExecAppendRequest> {
    let mut ssn_map = state.ssn_map.lock().await;
    let num_shards = u32::try_from(ssn_map.len()).unwrap();
    let ind = u64::try_from(ind).unwrap();

    // compute buckets and put into BTreeSet
    let step = u64::MAX / u64::from(num_shards);
    let mut rem = u64::MAX % u64::from(num_shards);
    let mut prev = 0;
    let mut buckets = BTreeMap::new();
    for i in 1..(num_shards + 1) {
        prev = prev + step;
        if rem > 0 {
            prev += 1;
            rem -= 1;
            buckets.insert(prev, (i, false));
        } else {
            buckets.insert(prev, (i, false));
        }
    }

    let mut res = HashMap::<u32, ExecAppendRequest>::new();
    let mut ind_to_sh = state.ind_to_sh.lock().await;
    let mut txn_queues = state.txn_queues.write().await;
    for (k, v) in write_set.into_iter() {
        let hash = xx::hash64(k.as_bytes());
        let mut after = buckets.range_mut((Excluded(hash), Unbounded));
        let (_, ub) = match after.next() {
            Some(ub) => ub,
            None => panic!("Greatest element of tree should be u64 MAX"),
        };
        if !ub.1 {
            // insert into transaction queue
            txn_queues
                .entry(ub.0)
                .and_modify(|ent| {
                    ent.1.push_back(ind);
                })
                .or_insert((0, VecDeque::from([ind])));

            // increment shard sequence number
            let ssn = ssn_map.get_mut(&ub.0).unwrap();
            *ssn += 1;

            // update ind to shard mapping
            ind_to_sh
                .entry(ind)
                .and_modify(|ent| {
                    ent.1.insert(ub.0);
                })
                .or_insert((cid, HashSet::from([ub.0])));

            // populate subtransaction
            match res.entry(ub.0) {
                Occupied(mut e) => {
                    e.get_mut().txn.insert(k, v);
                }
                Vacant(e) => {
                    e.insert(ExecAppendRequest {
                        txn: HashMap::new(),
                        ind,
                        sn: *ssn,
                        local_deps: None,
                    });
                }
            };

            // mark shard as visited
            *ub = (ub.0, true);
        }
    }
    return res;
}
