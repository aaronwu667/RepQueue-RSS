use proto::common_decls::{Csn, TxnRes};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::Channel;

pub mod cluster_management_service;
mod rpc_utils;
mod state_update;
pub mod transaction_service;

enum NodeStatus {
    Head(Channel),
    Tail(Channel),
    Middle(Channel, Channel),
}

enum TxnStatus {
    NotStarted(Arc<tokio::sync::Notify>),
    InProg(TransactionEntry),
    Done(TransactionEntry),
}

struct TransactionEntry {
    result: Option<TxnRes>,
    addr: String,
}

impl TransactionEntry {
    fn new(addr: String) -> Self {
        Self { result: None, addr }
    }

    fn new_res(result: Option<TxnRes>, addr: String) -> Self {
        Self { result, addr }
    }
}

struct ManagerNodeState {
    ongoing_txs: RwLock<HashMap<u64, BTreeMap<u64, TxnStatus>>>, // cid |-> (csn, statuses)
    txn_queues: RwLock<HashMap<u32, (u64, VecDeque<u64>)>>, // shard group id |-> (lastExec, queue), !!! transactions should start with csn 1 NOT 0 !!!
    ind_to_sh: Mutex<HashMap<u64, (Csn, HashSet<u32>)>>,    // log ind |-> (csn, shards)
    ssn_map: Mutex<HashMap<u32, u64>>, // !!! default value for SSN map should be 1 NOT 0 !!!
    num_shards: u32,                   // should agree with the above
    read_meta: Mutex<HashMap<u64, (u64, u64)>>, // cid |-> (max csn, lsn)
}

struct BucketEntry {
    sid: u32,
    visited: bool,
}

impl BucketEntry {
    fn new(sid: u32) -> Self {
        Self {
            sid,
            visited: false,
        }
    }
}

fn get_buckets(num_shards: u32) -> BTreeMap<u64, BucketEntry> {
    let step = u64::MAX / u64::from(num_shards);
    let mut rem = u64::MAX % u64::from(num_shards);
    let mut prev = 0;
    let mut buckets = BTreeMap::<u64, BucketEntry>::new();
    for i in 1..(num_shards + 1) {
        prev = prev + step;
        if rem > 0 {
            prev += 1;
            rem -= 1;
            buckets.insert(prev, BucketEntry::new(i));
        } else {
            buckets.insert(prev, BucketEntry::new(i));
        }
    }
    return buckets;
}
