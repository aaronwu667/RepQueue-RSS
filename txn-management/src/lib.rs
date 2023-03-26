use proto::common_decls::{Csn, TxnRes};
use proto::manager_net::manager_service_client::ManagerServiceClient;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::{Channel, Endpoint};

pub mod cluster_management_service;
mod rpc_utils;
mod state_update;
pub mod transaction_service;

enum ConnectionStatus {
    Init(Channel),
    NotInit(Endpoint),
}

pub struct Connection {
    inner: RwLock<ConnectionStatus>,
}

// c.f. channel pool
impl Connection {
    fn new(e: Endpoint) -> Self {
        Self {
            inner: RwLock::new(ConnectionStatus::NotInit(e)),
        }
    }
    async fn get_client(&self) -> ManagerServiceClient<Channel> {
        let status = self.inner.read().await;
        match &*status {
            ConnectionStatus::Init(ch) => ManagerServiceClient::new(ch.clone()),
            _ => panic!("Dynamic connection not implemented"),
        }
    }
    async fn connect(&self) -> Result<(), String> {
        let mut status = self.inner.write().await;
        match &*status {
            ConnectionStatus::NotInit(endpt) => match endpt.connect().await {
                Ok(ch) => {
                    *status = ConnectionStatus::Init(ch);
                    Ok(())
                }
                Err(_) => Err(format!("conenction failed with addr {}", endpt.uri())),
            },
            _ => Ok(()),
        }
    }
}

pub enum NodeStatus {
    Head(Arc<Connection>),
    Tail(Arc<Connection>),
    Middle(Arc<Connection>, Arc<Connection>),
}

impl NodeStatus {
    pub fn new_head(e: Endpoint) -> Self {
        Self::Head(Arc::new(Connection::new(e)))
    }
    pub fn new_tail(e: Endpoint) -> Self {
        Self::Tail(Arc::new(Connection::new(e)))
    }
    pub fn new_middle(pred: Endpoint, succ: Endpoint) -> Self {
        Self::Middle(
            Arc::new(Connection::new(pred)),
            Arc::new(Connection::new(succ)),
        )
    }
    pub async fn connect(&self) -> Result<(), String> {
        match self {
            Self::Head(c) => Ok(c.connect().await?),
            Self::Tail(c) => Ok(c.connect().await?),
            Self::Middle(c, d) => {
                c.connect().await?;
                d.connect().await?;
                Ok(())
            }
        }
    }
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

pub struct ManagerNodeState {
    ongoing_txs: RwLock<HashMap<u64, BTreeMap<u64, TxnStatus>>>, // cid |-> (csn, statuses)
    txn_queues: RwLock<HashMap<u32, (u64, VecDeque<u64>)>>, // shard group id |-> (lastExec, queue), !!! transactions should start with csn 1 NOT 0 !!!
    ind_to_sh: Mutex<HashMap<u64, (Csn, HashSet<u32>)>>,    // log ind |-> (csn, shards)
    ssn_map: Mutex<HashMap<u32, u64>>, // !!! default value for SSN map should be 1 NOT 0, initialized by client !!!
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
