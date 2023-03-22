use std::collections::{HashMap, BTreeMap, VecDeque, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use proto::common_decls::{TxnRes, Csn};
use replication::channel_pool::ChannelPool;
use tonic::transport::Channel;

pub mod transaction_service;
pub mod cluster_management_service;
mod state_update;
mod rpc_utils;
// static configuration of shards

enum TxnStatus {
    InProg,
    Done,
}

enum NodeStatus {
    Head(Channel),
    Tail(Channel, Arc<ChannelPool<u32>>),
    Middle(Channel, Channel),
}

struct ManagerNodeState {
    ongoing_txs: Mutex<HashMap<u64, BTreeMap<u64, (TxnStatus, Option<TxnRes>)>>>, // cid |-> (csn, statuses)
    txn_queues: RwLock<HashMap<u32, (u64, VecDeque<u64>)>>, // shard group id |-> (lastExec, queue)
    ind_to_sh: Mutex<HashMap<u64, (Csn, HashSet<u32>)>>,    // log ind |-> (cid, shards)
    ssn_map: Mutex<HashMap<u32, u64>>, // !!! default value for SSN map should be 1 NOT 0 !!!
}
