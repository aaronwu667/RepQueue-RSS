use super::TransactionService;
use super::{
    rpc_utils::{send_chain_rpc, send_client_rpc, send_cluster_rpc, RPCRequest},
    write_utils::{update_view, update_view_tail},
};
use super::{ManagerNodeState, TransactionEntry, TxnStatus};
use crate::debug;
use crate::{Connection, NodeStatus};
use proto::common_decls::Csn;
use proto::{
    client_lib::SessionRespWriteRequest,
    common_decls::{exec_notif_request::ReqStatus, ExecNotifRequest, TxnRes},
    manager_net::{AppendTransactRequest, ExecAppendTransactRequest},
};
use replication::channel_pool::ChannelPool;
use std::{
    collections::{hash_map::Entry::*, BTreeMap, HashMap, VecDeque},
    sync::Arc,
};
use tokio::sync::mpsc;

impl TransactionService {
    // TODO (low priority): periodic check for subtransactions to retry
    // TODO (high priority): join! instead of sequential awaits
    #[allow(dead_code)]
    async fn timeout_check() {
        todo!()
    }

    // nodes in the middle processing backwards ack
    pub(super) async fn proc_exec_append(
        state: Arc<ManagerNodeState>,
        mut exec_append_ch: mpsc::Receiver<ExecAppendTransactRequest>,
        pred: Option<Arc<Connection>>,
        client_conns: Arc<ChannelPool<u64>>,
    ) {
        loop {
            let req = exec_append_ch
                .recv()
                .await
                .expect("Exec append channel closed");
            // update queues
            let mut ind_to_sh = state.ind_to_sh.lock().await;
            let (csn, shards) = ind_to_sh
                .remove(&req.ind)
                .expect("No entry associated with log index");
            drop(ind_to_sh);

            let mut txn_queues = state.txn_queues.write().await;
            for sh in shards {
                let (last_exec, queue) = txn_queues
                    .get_mut(&sh)
                    .expect("No queue associated with shard group");
                while req.ind > *last_exec {
                    *last_exec = queue.pop_front().expect("Index should be in queue");
                }
            }
            drop(txn_queues);

            // update ongoing transactions
            debug("before ongoing txns".to_owned());
            let mut ongoing_txns = state.ongoing_txs.lock().await;
            let transaction = ongoing_txns
                .get_mut(&csn.cid)
                .and_then(|csn_map| csn_map.get_mut(&csn.sn))
                .expect("Missing appropriate entries in ongoing transactions");

            let addr = match transaction {
                TxnStatus::InProg(e) => {
                    let copy_addr = e.addr.to_owned();
                    debug(format!("ExecAppend {:?}", e));
                    *transaction = TxnStatus::Done(TransactionEntry::new_res(
                        e.result.clone(),
                        e.ind,
                        e.addr.to_owned(),
                    ));
                    copy_addr
                }
                TxnStatus::Done(e) => e.addr.to_owned(),
                _ => {
                    panic!("Finished transaction marked as not started")
                }
            };
            drop(ongoing_txns);
            debug("after ongoing txns".to_owned());

            // send result to client if head, otherwise continue backwards ack
            if let Some(ref ch) = pred {
                send_chain_rpc(RPCRequest::ExecAppendTransact(req), ch.clone()).await;
            } else {
                let cid = csn.cid;
                let resp = SessionRespWriteRequest {
                    res: req.res,
                    csn: Some(csn),
                };
                send_client_rpc(
                    RPCRequest::SessResponseWrite(resp),
                    cid,
                    addr,
                    client_conns.clone(),
                )
                .await;
            }
        }
    }

    // processes results as they come in from cluster
    pub(super) async fn aggregate_res(
        state: Arc<ManagerNodeState>,
        mut exec_ch: mpsc::Receiver<ExecNotifRequest>,
        pred: Arc<Connection>,
    ) {
        loop {
            let partial_res = exec_ch
                .recv()
                .await
                .expect("ExecNotif channel sender closed");
            // update queues
            let partial_res = match partial_res.req_status.expect("Should have a status") {
                ReqStatus::Response(r) => r,
                _ => panic!("Wrong type of message for execnotif"),
            };
            let mut txn_queues = state.txn_queues.write().await;
            match txn_queues.get_mut(&partial_res.shard_id) {
                Some(queue) => {
                    while partial_res.ind > queue.0 {
                        queue.0 = queue.1.pop_front().expect("Queue should not be empty");
                    }
                }
                None => panic!("Missing entry in txn_queues map"),
            }
            drop(txn_queues);

            // update ind to shard map and get cwsn
            let mut done = false;
            let mut ind_to_sh = state.ind_to_sh.lock().await;
            let (cid, csn_num) = match ind_to_sh.get_mut(&partial_res.ind) {
                Some(e) => {
                    e.1.remove(&partial_res.shard_id);
                    if e.1.is_empty() {
                        done = true;
                    }
                    (e.0.cid, e.0.sn)
                }
                None => panic!("Missing entry in ind-to-shard map"),
            };
            if done {
                ind_to_sh.remove(&partial_res.ind);
            }
            drop(ind_to_sh);

            // update ongoing txn map
            let mut ongoing_txns = state.ongoing_txs.lock().await;
            let transaction = match ongoing_txns
                .get_mut(&cid)
                .and_then(|csn_map| csn_map.get_mut(&csn_num))
            {
                Some(e) => e,
                None => {
                    debug(format!("Ongoing transaction map: {:?}", ongoing_txns));
                    panic!("Missing entries in ongoing txns");
                }
            };

            // view consistency check
            let transact_ent = match transaction {
                TxnStatus::InProg(e) => e,
                TxnStatus::Done(e) => {
                    if !done {
                        panic!("Transaction marked as done, but responses not recieved from all shards")
                    }
                    e
                }
                TxnStatus::NotStarted(_, _) => {
                    panic!("Transaction marked as not started is being executed")
                }
            };
            // merge results into existing
            match (partial_res.res, transact_ent.result.as_mut()) {
                (Some(res), Some(resp_data)) => {
                    for (k, v) in res.map {
                        resp_data.map.insert(k, v);
                    }
                }
                (Some(res), None) => {
                    transact_ent.result = Some(TxnRes { map: res.map });
                }
                (None, None) | (None, Some(_)) => (),
            };

            // backwards ack through chain if done
            if done {
                let res = transact_ent.result.clone();
                *transaction = TxnStatus::Done(TransactionEntry::new_res(
                    res.clone(),
                    transact_ent.ind,
                    transact_ent.addr.to_owned(),
                ));
                let req = ExecAppendTransactRequest {
                    ind: partial_res.ind,
                    res,
                };
                tokio::spawn(send_chain_rpc(
                    RPCRequest::ExecAppendTransact(req),
                    pred.clone(),
                ));
            }
            drop(ongoing_txns);
        }
    }

    pub(super) async fn proc_append(
        mut schd_rx: mpsc::Receiver<AppendTransactRequest>,
        state: Arc<ManagerNodeState>,
        node_state: Arc<NodeStatus>,
        cluster_conns: Arc<ChannelPool<u32>>,
    ) {
        let mut log = VecDeque::<AppendTransactRequest>::new();

        // Blank entry so that all sequence numbers are consistent
        log.push_back(AppendTransactRequest {
            txn: HashMap::new(),
            csn: Some(Csn { cid: 0, sn: 0 }),
            ack_bound: 0,
            ind: 0,
            addr: "".to_owned(),
        });

        loop {
            let released_req = schd_rx.recv().await.expect("Schedule sender dropped");
            let ind = u64::try_from(log.len()).unwrap();
            log.push_back(released_req.clone());
            debug(format!("Last log index {}", ind));
            let csn = released_req.csn.as_ref().expect("Missing csn");
            let addr = released_req.addr.clone();
            let mut ongoing_txns = state.ongoing_txs.lock().await;
            match ongoing_txns.entry(csn.cid) {
                Occupied(mut o) => {
                    if let Some(TxnStatus::NotStarted(_, notif)) = o
                        .get_mut()
                        .insert(csn.sn, TxnStatus::InProg(TransactionEntry::new(ind, addr)))
                    {
                        debug(format!("Resolving any reads waiting on CWSN {}", csn.sn));
                        if notif.send(ind).is_err() {
                            panic!("All watch receivers dropped")
                        }
                    }

                    o.get_mut().retain(|k, _| *k >= released_req.ack_bound);
                }
                Vacant(v) => {
                    v.insert(BTreeMap::from([(
                        csn.sn,
                        TxnStatus::InProg(TransactionEntry::new(ind, addr)),
                    )]));
                }
            };
            drop(ongoing_txns);
            match &*node_state {
                NodeStatus::Head(succ) => {
                    update_view(&state, ind, csn.clone(), &released_req.txn).await;
                    let new_req = AppendTransactRequest {
                        ind,
                        ..released_req
                    };
                    tokio::spawn(send_chain_rpc(
                        RPCRequest::AppendTransact(new_req),
                        succ.clone(),
                    ));
                }
                NodeStatus::Middle(_, succ) => {
                    update_view(&state, ind, csn.clone(), &released_req.txn).await;
                    tokio::spawn(send_chain_rpc(
                        RPCRequest::AppendTransact(released_req),
                        succ.clone(),
                    ));
                }
                NodeStatus::Tail(_) => {
                    let reqs = update_view_tail(&state, ind, csn.clone(), released_req.txn).await;
                    for (k, v) in reqs.into_iter() {
                        tokio::spawn(send_cluster_rpc(
                            k,
                            RPCRequest::ExecAppend(v),
                            cluster_conns.clone(),
                        ));
                    }
                }
            }
        }
    }

    // if head. can push into log in any order, else need to
    // do another round of serialization
    pub(super) async fn log_send_or_queue(
        schd_tx: &mpsc::Sender<AppendTransactRequest>,
        req: AppendTransactRequest,
        log_ind: &mut u64,
        log_queue: &mut BTreeMap<u64, AppendTransactRequest>,
        node_state: Arc<NodeStatus>,
    ) {
        if let NodeStatus::Head(_) = *node_state {
            schd_tx
                .send(req)
                .await
                .expect("schd_ch receiver should not have dropped");
            *log_ind += 1;
        } else if req.ind == *log_ind {
            schd_tx
                .send(req)
                .await
                .expect("schd_ch receiver should not have dropped");
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
                schd_tx
                    .send(head)
                    .await
                    .expect("schd_ch receiver should not have dropped");
            }
            *log_ind = new_ind;
        } else {
            log_queue.insert(req.ind, req);
        }
    }

    pub(super) async fn scheduler(
        mut new_req_rx: mpsc::Receiver<AppendTransactRequest>,
        schd_tx: mpsc::Sender<AppendTransactRequest>,
        state: Arc<ManagerNodeState>,
        node_state: Arc<NodeStatus>,
        client_conns: Arc<ChannelPool<u64>>,
    ) {
        let mut log_ind = 1;
        // queues for deciding when to service
        let mut client_queue = HashMap::<u64, (u64, BTreeMap<u64, AppendTransactRequest>)>::new();
        let mut log_queue = BTreeMap::<u64, AppendTransactRequest>::new();

        loop {
            let new_req = new_req_rx.recv().await.expect("Append req channel closed");
            let csn = new_req.csn.as_ref().expect("Missing csn");
            match client_queue.entry(csn.cid) {
                Occupied(mut o) => {
                    let (greatest_csn, queue) = o.get_mut();
                    if csn.sn == *greatest_csn + 1 {
                        // If no reordering, check for log reordering and service
                        Self::log_send_or_queue(
                            &schd_tx,
                            new_req,
                            &mut log_ind,
                            &mut log_queue,
                            node_state.clone(),
                        )
                        .await;
                        *greatest_csn += 1;

                        // see what else can be serviced
                        let mut curr_sn = *greatest_csn;
                        let mut unblocked_reqs = Vec::with_capacity(queue.len() / 3);
                        for sn in queue.keys() {
                            if *sn == curr_sn + 1 {
                                unblocked_reqs.push(*sn);
                                curr_sn += 1;
                            } else {
                                break;
                            }
                        }

                        for k in unblocked_reqs.into_iter() {
                            let client_queue_head = queue.remove(&k).unwrap();
                            Self::log_send_or_queue(
                                &schd_tx,
                                client_queue_head,
                                &mut log_ind,
                                &mut log_queue,
                                node_state.clone(),
                            )
                            .await;
                        }
                        *greatest_csn = curr_sn;
                    } else if csn.sn <= *greatest_csn {
                        // Have seen this sequence number before
                        // Check if we have a response ready
                        let ongoing_txns = state.ongoing_txs.lock().await;
                        if let (NodeStatus::Head(_), Some(csn_map)) =
                            (&*node_state, ongoing_txns.get(&csn.cid))
                        {
                            if let Some(TxnStatus::Done(txn_entry)) = csn_map.get(&csn.sn) {
                                let resp = SessionRespWriteRequest {
                                    csn: Some(csn.clone()),
                                    res: txn_entry.result.clone(),
                                };
                                tokio::spawn(send_client_rpc(
                                    RPCRequest::SessResponseWrite(resp),
                                    csn.cid,
                                    new_req.addr,
                                    client_conns.clone(),
                                ));
                            }
                        }
                    } else {
                        // Otherwise, put onto queue for waiting
                        queue.insert(csn.sn, new_req);
                    }
                }
                Vacant(v) => {
                    let mut last_exec = 0;
                    let mut new_map = BTreeMap::new();
                    if csn.sn == 1 {
                        Self::log_send_or_queue(
                            &schd_tx,
                            new_req,
                            &mut log_ind,
                            &mut log_queue,
                            node_state.clone(),
                        )
                        .await;
                        last_exec = 1;
                    } else {
                        new_map.insert(csn.sn, new_req);
                    }
                    v.insert((last_exec, new_map));
                }
            }
        }
    }
}
