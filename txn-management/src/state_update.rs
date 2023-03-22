use crate::{get_buckets, ManagerNodeState};
use fasthash::xx;
use proto::common_decls::{Csn, TransactionOp};
use proto::shard_net::ExecAppendRequest;
use std::collections::hash_map::Entry::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Bound::*;

// utils for transaction service
pub(crate) async fn update_view(
    state: &ManagerNodeState,
    ind: usize,
    csn: Csn,
    write_set: &HashMap<String, TransactionOp>,
) {
    let ind = u64::try_from(ind).unwrap();
    let mut buckets = get_buckets(state.num_shards);

    let mut ind_to_sh = state.ind_to_sh.lock().await;
    let mut txn_queues = state.txn_queues.write().await;
    let mut ssn_map = state.ssn_map.lock().await;
    for k in write_set.keys() {
        // hashing
        let hash = xx::hash64(k.as_bytes());
        let mut after = buckets.range_mut((Excluded(hash), Unbounded));
        let (_, ub) = match after.next() {
            Some(ub) => ub,
            None => panic!("Greatest element of tree should be u64 MAX"),
        };
        if !ub.visited {
            // insert into transaction queue
            txn_queues
                .entry(ub.sid)
                .and_modify(|ent| {
                    ent.1.push_back(ind);
                })
                .or_insert((0, VecDeque::from([ind])));

            // increment shard sequence number
            let ssn = ssn_map.get_mut(&ub.sid).unwrap();
            *ssn += 1;

            // update ind to shard mapping
            ind_to_sh
                .entry(ind)
                .and_modify(|ent| {
                    ent.1.insert(ub.sid);
                })
                .or_insert((csn.clone(), HashSet::from([ub.sid])));

            // mark shard as visited
            ub.visited = true;
        }
    }
}

// TODO(med priority): dependency analysis in loop
pub(crate) async fn update_view_tail(
    state: &ManagerNodeState,
    ind: usize,
    csn: Csn,
    write_set: HashMap<String, TransactionOp>,
) -> HashMap<u32, ExecAppendRequest> {
    let ind = u64::try_from(ind).unwrap();

    // compute buckets and put into BTreeSet
    let mut buckets = get_buckets(state.num_shards);

    let mut res = HashMap::<u32, ExecAppendRequest>::new();
    let mut ind_to_sh = state.ind_to_sh.lock().await;
    let mut txn_queues = state.txn_queues.write().await;
    let mut ssn_map = state.ssn_map.lock().await;
    for (k, v) in write_set.into_iter() {
        let hash = xx::hash64(k.as_bytes());
        let mut after = buckets.range_mut((Excluded(hash), Unbounded));
        let (_, ub) = match after.next() {
            Some(ub) => ub,
            None => panic!("Greatest element of tree should be u64 MAX"),
        };
        if !ub.visited {
            // insert into transaction queue
            txn_queues
                .entry(ub.sid)
                .and_modify(|ent| {
                    ent.1.push_back(ind);
                })
                .or_insert((0, VecDeque::from([ind])));

            // increment shard sequence number
            let ssn = ssn_map.get_mut(&ub.sid).unwrap();
            *ssn += 1;

            // update ind to shard mapping
            ind_to_sh
                .entry(ind)
                .and_modify(|ent| {
                    ent.1.insert(ub.sid);
                })
                .or_insert((csn.clone(), HashSet::from([ub.sid])));

            // populate subtransaction
            match res.entry(ub.sid) {
                Occupied(mut e) => {
                    e.get_mut().txn.insert(k, v);
                }
                Vacant(e) => {
                    e.insert(ExecAppendRequest {
                        txn: HashMap::from([(k, v)]),
                        ind,
                        sn: *ssn,
                        local_deps: None,
                    });
                }
            };

            // mark shard as visited
            ub.visited = true;
        }
    }
    return res;
}
