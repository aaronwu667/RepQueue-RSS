use super::sharding::get_buckets;
use super::ManagerNodeState;
use fasthash::xx;
use proto::common_decls::{Csn, TransactionOp};
use proto::shard_net::ExecAppendRequest;
use std::collections::hash_map::Entry::*;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Bound::*;

// utils for transaction service
pub(super) async fn update_view(
    state: &ManagerNodeState,
    ind: u64,
    csn: Csn,
    write_set: &HashMap<String, TransactionOp>,
) {
    // Since flush messages aren't implemented yet, we need to send
    // transactions to every group, irrespective of sharding.
    //let mut buckets = get_buckets(state.num_shards);

    let mut ind_to_sh = state.ind_to_sh.lock().await;
    let mut txn_queues = state.txn_queues.write().await;
    let mut ssn_map = state.ssn_map.lock().await;
    for sh in ssn_map.iter_mut() {
        txn_queues.entry(*sh.0).and_modify(|ent| ent.1.push_back(ind)).or_insert((0, VecDeque::from([ind])));
        *sh.1 += 1;
        ind_to_sh.entry(ind).and_modify(|ent| {ent.1.insert(*sh.0);}).or_insert((csn.clone(), HashSet::from([*sh.0])));
        
    }
    /*
    for k in write_set.keys() {
        // get responsible shard
        let hash = xx::hash64(k.as_bytes());
        let mut after = buckets.range_mut((Excluded(hash), Unbounded));
        let (_, ub) = match after.next() {
            Some(ub) => ub,
            None => panic!("Greatest element of tree should be u64 MAX"),
        };

        // same as update_view_tail minus populating subtransaction
        if !ub.visited {
            // insert into transaction queue
            txn_queues
                .entry(ub.sid)
                .and_modify(|ent| {
                    ent.1.push_back(ind);
                })
                .or_insert((0, VecDeque::from([ind])));

            // increment shard sequence number
            let ssn = ssn_map
                .get_mut(&ub.sid)
                .expect("SSN entry for shard should be present at initialization");
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
    */
}

// TODO(med priority): dependency analysis in loop
pub(super) async fn update_view_tail(
    state: &ManagerNodeState,
    ind: u64,
    csn: Csn,
    write_set: HashMap<String, TransactionOp>,
) -> HashMap<u32, ExecAppendRequest> {
    // compute buckets and put into BTreeSet
    //let mut buckets = get_buckets(state.num_shards);

    let mut res = HashMap::<u32, ExecAppendRequest>::new();
    let mut ind_to_sh = state.ind_to_sh.lock().await;
    let mut txn_queues = state.txn_queues.write().await;
    let mut ssn_map = state.ssn_map.lock().await;
    for sh in ssn_map.iter_mut() {
        txn_queues.entry(*sh.0).and_modify(|ent| ent.1.push_back(ind)).or_insert((0, VecDeque::from([ind])));
        *sh.1 += 1;
        ind_to_sh.entry(ind).and_modify(|ent| {ent.1.insert(*sh.0);}).or_insert((csn.clone(), HashSet::from([*sh.0])));
        res.insert(*sh.0, ExecAppendRequest { txn: write_set.clone(), ind, sn: *sh.1, local_deps: None });
    }
    res
    /*
    for (k, v) in write_set.into_iter() {
        // Get shard responsible for key
        let hash = xx::hash64(k.as_bytes());
        let mut after = buckets.range_mut((Excluded(hash), Unbounded));
        let (_, ub) = after
            .next()
            .expect("Greatest element of tree should be u64 MAX");

        // fetch shard sequence number
        let ssn = ssn_map
            .get_mut(&ub.sid)
            .expect("SSN entry for shard should be present at initialization");

        // update manager data structures and sequence number if not already visited
        if !ub.visited {
            // insert into transaction queue
            txn_queues
                .entry(ub.sid)
                .and_modify(|ent| {
                    ent.1.push_back(ind);
                })
                .or_insert((0, VecDeque::from([ind])));

            // increment shard sequence number
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
    }
    res
    */
}
