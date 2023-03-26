use crate::NodeStatus;
use async_trait::async_trait;
use proto::common_decls::{Csn, TxnRes};
use proto::{
    common_decls::{Empty, ExecNotifRequest},
    manager_net::{
        manager_service_server::ManagerService, AppendTransactRequest, ExecAppendTransactRequest,
        ReadOnlyTransactRequest,
    },
};
use replication::channel_pool::ChannelPool;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::{Request, Response, Status};

mod read_transaction;
mod read_utils;
mod rpc_utils;
mod write_utils;
mod write_transaction;
mod sharding;

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
    ssn_map: Mutex<HashMap<u32, u64>>, // !!! default value for SSN map should be 1 NOT 0, initialized by client !!!
    num_shards: u32,                   // should agree with the above
    read_meta: Mutex<HashMap<u64, (u64, u64)>>, // cid |-> (max csn, lsn)
}

impl ManagerNodeState {
    fn new(num_shards: u32) -> Self {
        let mut ssn_map = HashMap::new();
        for i in 0..num_shards {
            ssn_map.insert(i, 1);
        }
        let ongoing_txs = RwLock::new(HashMap::new());
        let txn_queues = RwLock::new(HashMap::new());
        let ind_to_sh = Mutex::new(HashMap::new());
        let ssn_map = Mutex::new(ssn_map);
        let read_meta = Mutex::new(HashMap::new());
        ManagerNodeState {
            ongoing_txs,
            txn_queues,
            ind_to_sh,
            ssn_map,
            num_shards,
            read_meta,
        }
    }
}

pub struct TransactionService {
    // TODO (low priority): timeout checking
    // TODO (low priority): dynamic reconfig and failover
    // TODO (med priority): fair queueing for read-write transactions
    state: Arc<ManagerNodeState>,
    cluster_conns: Arc<ChannelPool<u32>>,
    new_rq_ch: mpsc::Sender<AppendTransactRequest>,
    exec_notif_ch: Option<mpsc::Sender<ExecNotifRequest>>,
    exec_append_ch: Option<mpsc::Sender<ExecAppendTransactRequest>>,
}

impl TransactionService {
    pub fn new(
        num_shards: u32,
        cluster_conns: Arc<ChannelPool<u32>>,
        node_status: Arc<NodeStatus>,
    ) -> Self {
        let state = Arc::new(ManagerNodeState::new(num_shards));
        let (new_req_tx, new_req_rx) = mpsc::channel(5000);
        let (schd_tx, schd_rx) = mpsc::channel(5000);
        let mut exec_tx = None; // RPC handler -> exec notif servicer sender
        let mut exec_append_tx = None;
        match &*node_status {
            NodeStatus::Tail(pred) => {
                // spawn execNotif handler
                let (tx, exec_notif_rx) = mpsc::channel(8000);
                tokio::spawn(Self::aggregate_res(
                    state.clone(),
                    exec_notif_rx,
                    pred.clone(),
                ));
                exec_tx = Some(tx);
            }
            NodeStatus::Middle(pred, _) => {
                let (tx, exec_append_rx) = mpsc::channel(8000);
                tokio::spawn(Self::proc_exec_append(
                    state.clone(),
                    exec_append_rx,
                    Some(pred.clone()),
                ));
                exec_append_tx = Some(tx);
            }
            NodeStatus::Head(_) => {
                let (tx, exec_append_rx) = mpsc::channel(8000);
                tokio::spawn(Self::proc_exec_append(state.clone(), exec_append_rx, None));
                exec_append_tx = Some(tx);
            }
        }
        tokio::spawn(Self::scheduler(
            state.clone(),
            new_req_rx,
            schd_tx,
            node_status.clone(),
        ));
        tokio::spawn(Self::proc_append(
            schd_rx,
            state.clone(),
            node_status,
            cluster_conns.clone(),
        ));
        Self {
            state,
            cluster_conns,
            new_rq_ch: new_req_tx,
            exec_notif_ch: exec_tx,
            exec_append_ch: exec_append_tx,
        }
    }
}

#[async_trait]
impl ManagerService for TransactionService {
    async fn append_transact(
        &self,
        request: Request<AppendTransactRequest>,
    ) -> Result<Response<Empty>, Status> {
        // TODO (low priority): handle dynamic addition/removal of head (watch on node state)
        if let Err(_) = self.new_rq_ch.send(request.into_inner()).await {
            panic!("New request receiver dropped");
        }

        Ok(Response::new(Empty {}))
    }

    async fn exec_notif(
        &self,
        request: Request<ExecNotifRequest>,
    ) -> Result<Response<Empty>, Status> {
        // TODO (low priority): handle dynamic addition/removal of tail (watch on node state)
        match &self.exec_notif_ch {
            Some(c) => {
                if let Err(_) = c.send(request.into_inner()).await {
                    panic!("Exec notif receiver dropped");
                }
                Ok(Response::new(Empty {}))
            }
            None => Err(Status::new(
                tonic::Code::InvalidArgument,
                "Node is not tail",
            )),
        }
    }

    async fn exec_append_transact(
        &self,
        request: Request<ExecAppendTransactRequest>,
    ) -> Result<Response<Empty>, Status> {
        match &self.exec_append_ch {
            Some(c) => {
                if let Err(_) = c.send(request.into_inner()).await {
                    panic!("Exec append receiver dropped");
                }
                Ok(Response::new(Empty {}))
            }
            None => Err(Status::new(tonic::Code::InvalidArgument, "Node is tail")),
        }
    }

    async fn read_only_transact(
        &self,
        request: Request<ReadOnlyTransactRequest>,
    ) -> Result<Response<Empty>, Status> {
        tokio::spawn(Self::proc_read(
            request.into_inner(),
            self.state.clone(),
            self.cluster_conns.clone(),
        ));
        Ok(Response::new(Empty {}))
    }
}
