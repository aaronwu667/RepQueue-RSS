use std::{
    collections::{hash_map::Entry::*, BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use proto::{
    common_decls::{Csn, Empty, TxnRes},
    manager_net::{
        manager_service_client::ManagerServiceClient, manager_service_server::ManagerService,
        AppendTransactRequest, ExecAppendTransactRequest, ExecNotifRequest,
        ReadOnlyTransactRequest,
    },
};
use replication::channel_pool::ChannelPool;
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::{transport::Channel, Request, Response, Status};

// need to include cid for head failover
pub enum TxnStatus {
    InProg(TxnRes),
    Done(TxnRes),
}

pub enum NodeStatus {
    Head(Channel),
    Tail(Channel, ChannelPool),
    Middle(Channel, Channel),
}

struct ManagerNodeState {
    ongoing_txs: Mutex<HashMap<u64, HashSet<TxnStatus>>>, // cid |-> statuses
    txn_queues: RwLock<HashMap<u32, (u64, VecDeque<u64>)>>, // shard group id |-> (lastExec, queue)
    ind_to_sh: Mutex<HashMap<u64, (u64, HashSet<u32>)>>,  // log ind |-> (cid, shards)
    ssn_map: Mutex<HashMap<u32, u64>>, // default value for SSN map should be 1 NOT 0
}

struct TransactionService {
    // TODO (low priority): dynamic reconfig and failover
    // TODO (low priority): no lock timeout checking
    // TODO (med priority): fair queueing for read-write transactions
    state: Arc<ManagerNodeState>,
    new_rq_ch: mpsc::Sender<AppendTransactRequest>,
    exec_notif_ch: Option<mpsc::Sender<ExecNotifRequest>>,
}

impl TransactionService {
    fn new(
        pred: Option<Channel>,
        succ: Option<Channel>,
        cluster_conns: ChannelPool,
        node_state: NodeStatus,
    ) -> Self {
        let (new_req_tx, new_req_rx) = mpsc::channel(5000);
        let (schd_tx, schd_rx) = mpsc::channel(5000);
        let mut exec_tx = None; // RPC handler -> exec notif servicer sender
        let node_state = Arc::new(node_state);
        match &*node_state {
            NodeStatus::Tail(pred, _) => {
                // spawn execNotif handler
                let (exec_notif_tx, exec_notif_rx) = mpsc::channel(1000);
                tokio::spawn(TransactionService::aggregate_res(
                    exec_notif_rx,
                    pred.clone(),
                ));
                exec_tx = Some(exec_notif_tx);
            }
            _ => (),
        }
        let state = Arc::new(ManagerNodeState {
            ongoing_txs: Mutex::new(HashMap::new()),
            txn_queues: RwLock::new(HashMap::new()),
            ind_to_sh: Mutex::new(HashMap::new()),
            ssn_map: Mutex::new(HashMap::new()),
        });
        tokio::spawn(Self::scheduler(new_req_rx, schd_tx, node_state.clone()));
        tokio::spawn(Self::proc_rw(schd_rx, state.clone(), node_state));
        Self {
            state,
            new_rq_ch: new_req_tx,
            exec_notif_ch: exec_tx,
        }
    }

    // periodic check for subtransactions to retry
    async fn timeout_check() {}

    // processes results as they come in from cluster
    async fn aggregate_res(mut exec_ch: mpsc::Receiver<ExecNotifRequest>, pred: Channel) {}

    async fn proc_rw(
        mut proc_ch: mpsc::Receiver<AppendTransactRequest>,
        state: Arc<ManagerNodeState>,
        node_state: Arc<NodeStatus>,
    ) {
        let mut log = VecDeque::<AppendTransactRequest>::new();

        loop {
            let released_req = proc_ch.recv().await;
            if let Some(released_req) = released_req {
                let ind = log.len();
                log.push_back(released_req.clone());
                let ongoing_txns = state.ongoing_txs.lock().await;
                // TODO
                //ongoing_txns.insert(k, v);
                //ongoing_txns.retain(f);
                drop(ongoing_txns);

                let ind_to_sh = state.ind_to_sh.lock().await;
                let ssn_map = state.ssn_map.lock().await;
                // TODO
                drop(ssn_map);
                drop(ind_to_sh);

                match &*node_state {
                    NodeStatus::Head(succ) => {
                        let mut client = ManagerServiceClient::new(succ.clone());
                        let new_req = AppendTransactRequest {
                            ind: u64::try_from(ind).unwrap(),
                            ..released_req
                        };
                        if let Err(e) = client.append_transact(Request::new(new_req)).await {
                            eprintln!("Chain replication failed: {}", e);
                        }
                    }
                    NodeStatus::Middle(_, succ) => {
                        let mut client = ManagerServiceClient::new(succ.clone());
                        if let Err(e) = client.append_transact(Request::new(released_req)).await {
                            eprintln!("Chain replication failed: {}", e);
                        }
                    }
                    NodeStatus::Tail(_, cluster) => {// TODO
                    }
                }
            } else {
                panic!("Processing channel sender closed")
            }
        }
    }

    // if head. can push into log in any order, else need to
    // do another round of serialization
    async fn log_send_or_queue(
        req: AppendTransactRequest,
        log_ind: &mut u64,
        log_queue: &mut BTreeMap<u64, AppendTransactRequest>,
        proc_ch: &mpsc::Sender<AppendTransactRequest>,
        node_state: Arc<NodeStatus>,
    ) {
        if let NodeStatus::Head(_) = *node_state {
            proc_ch.send(req).await.unwrap();
            *log_ind += 1;
        } else if req.ind == *log_ind {
            proc_ch.send(req).await.unwrap();
            *log_ind += 1;

            // send out continuous prefix of log queue
            let mut log_ents = Vec::new();
            let mut new_ind = *log_ind;
            for ind in log_queue.keys() {
                if *ind == new_ind {
                    log_ents.push(*ind);
                    new_ind += 1;
                } else {
                    break;
                }
            }

            for ind in log_ents.into_iter() {
                let head = log_queue.remove(&ind).unwrap();
                proc_ch.send(head).await.unwrap();
            }
            *log_ind = new_ind;
        } else {
            log_queue.insert(req.ind, req);
        }
    }

    async fn scheduler(
        mut append_ch: mpsc::Receiver<AppendTransactRequest>,
        proc_ch: mpsc::Sender<AppendTransactRequest>,
        node_state: Arc<NodeStatus>,
    ) {
        let mut log_ind = 0;
        // queues for deciding when to service
        let mut client_queue = HashMap::<u64, (u64, BTreeMap<u64, AppendTransactRequest>)>::new();
        let mut log_queue = BTreeMap::<u64, AppendTransactRequest>::new();

        loop {
            let new_req = append_ch.recv().await;
            if let Some(new_req) = new_req {
                let Csn { cid, sn: csn_num } = new_req.csn.as_ref().unwrap();
                match client_queue.entry(*cid) {
                    Occupied(mut o) => {
                        if *csn_num == o.get().0 + 1 {
                            Self::log_send_or_queue(
                                new_req,
                                &mut log_ind,
                                &mut log_queue,
                                &proc_ch,
                                node_state.clone(),
                            )
                            .await;
                            (*o.get_mut()).0 += 1;

                            // see what else can be serviced
                            let mut curr_sn = o.get().0;
                            let mut unblocked_reqs = Vec::with_capacity(o.get().1.len() / 3);
                            for sn in o.get().1.keys() {
                                if *sn == curr_sn + 1 {
                                    unblocked_reqs.push(*sn);
                                    curr_sn += 1;
                                } else {
                                    break;
                                }
                            }

                            for k in unblocked_reqs.into_iter() {
                                let client_queue_head = o.get_mut().1.remove(&k).unwrap();
                                Self::log_send_or_queue(
                                    client_queue_head,
                                    &mut log_ind,
                                    &mut log_queue,
                                    &proc_ch,
                                    node_state.clone(),
                                )
                                .await;
                            }
                            (*o.get_mut()).0 = curr_sn;
                        } else {
                            o.get_mut().1.insert(*csn_num, new_req);
                        }
                    }
                    Vacant(v) => {
                        let mut last_exec = 0;
                        let mut new_map = BTreeMap::new();
                        if *csn_num == 1 {
                            Self::log_send_or_queue(
                                new_req,
                                &mut log_ind,
                                &mut log_queue,
                                &proc_ch,
                                node_state.clone(),
                            )
                            .await;
                            last_exec = 1;
                        } else {
                            new_map.insert(*csn_num, new_req);
                        }
                        v.insert((last_exec, new_map));
                    }
                }
            } else {
                panic!("Append req channel closed");
            }
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
            _ => Err(Status::new(
                tonic::Code::InvalidArgument,
                "Node is not tail",
            )),
        }
    }

    async fn exec_append_transact(
        &self,
        request: Request<ExecAppendTransactRequest>,
    ) -> Result<Response<Empty>, Status> {
        todo!()
    }

    async fn read_only_transact(
        &self,
        request: tonic::Request<ReadOnlyTransactRequest>,
    ) -> Result<Response<Empty>, Status> {
        todo!()
    }
}
