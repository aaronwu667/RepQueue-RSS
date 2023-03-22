use super::TransactionService;
use crate::{
    rpc_utils::{send_chain_rpc, send_client_rpc, send_cluster_rpc, RPCRequest},
    state_update::{update_view, update_view_tail},
    ManagerNodeState, NodeStatus, TransactionEntry, TxnStatus,
};
use proto::{
    client_lib::{SessionRespReadRequest, SessionRespWriteRequest},
    common_decls::{exec_notif_request::ReqStatus, Csn, ExecNotifRequest, TxnRes},
    manager_net::{AppendTransactRequest, ExecAppendTransactRequest},
    shard_net::ExecAppendRequest,
};
use replication::channel_pool::ChannelPool;
use std::{
    collections::{hash_map::Entry::*, BTreeMap, HashMap, VecDeque},
    sync::Arc,
};
use tokio::sync::mpsc;
use tonic::transport::Channel;

impl TransactionService {
    // TODO (low priority): periodic check for subtransactions to retry
    #[allow(dead_code)]
    async fn timeout_check() {
        todo!()
    }

    // nodes in the middle processing backwards ack
    pub(super) async fn proc_exec_append(
        state: Arc<ManagerNodeState>,
        mut exec_append_ch: mpsc::Receiver<ExecAppendTransactRequest>,
        pred: Option<Channel>,
    ) {
        loop {
            if let Some(req) = exec_append_ch.recv().await {
                // update queues
                let mut ind_to_sh = state.ind_to_sh.lock().await;
                let (csn, shards) = match ind_to_sh.remove(&req.ind) {
                    Some(ent) => ent,
                    None => panic!("No entry associated with log index"),
                };
                drop(ind_to_sh);

                let mut txn_queues = state.txn_queues.write().await;
                for sh in shards {
                    let (last_exec, queue) = match txn_queues.get_mut(&sh) {
                        Some(ent) => ent,
                        None => panic!("No queue associated with shard group"),
                    };
                    while req.ind > *last_exec {
                        *last_exec = queue.pop_front().unwrap();
                    }
                }
                drop(txn_queues);

                // update ongoing transactions
                let mut ongoing_txns = state.ongoing_txs.write().await;
                let transaction = match ongoing_txns
                    .get_mut(&csn.cid)
                    .and_then(|csn_map| csn_map.get_mut(&csn.sn))
                {
                    Some(s) => s,
                    None => panic!("Missing appropriate entries in ongoing transactions"),
                };

                let addr = match transaction {
                    TxnStatus::InProg(e) => {
                        let copy_addr = e.addr.to_owned();
                        *transaction = TxnStatus::Done(TransactionEntry::new_res(
                            req.res.clone(),
                            copy_addr.clone(),
                        ));
                        copy_addr
                    }
                    TxnStatus::Done(e) => e.addr.to_owned(),
                    _ => {
                        panic!("Finished transaction marked as not started")
                    }
                };
                drop(ongoing_txns);

                // send result to client if head, otherwise continue backwards ack
                if let Some(ref ch) = pred {
                    send_chain_rpc(RPCRequest::ExecAppendTransact(req), ch.clone()).await;
                } else {
                    let resp = SessionRespWriteRequest {
                        res: req.res,
                        csn: Some(csn),
                    };
                    send_client_rpc(RPCRequest::SessResponseWrite(resp), addr).await;
                }
            } else {
                panic!("Exec append channel closed");
            }
        }
    }

    // processes results as they come in from cluster
    pub(super) async fn aggregate_res(
        state: Arc<ManagerNodeState>,
        mut exec_ch: mpsc::Receiver<ExecNotifRequest>,
        pred: Channel,
    ) {
        loop {
            if let Some(partial_res) = exec_ch.recv().await {
                // update queues
                let partial_res = match partial_res.req_status.unwrap() {
                    ReqStatus::Response(r) => r,
                    _ => panic!("Wrong type of message for execnotif"),
                };
                let mut txn_queues = state.txn_queues.write().await;
                match txn_queues.get_mut(&partial_res.shard_id) {
                    Some(queue) => {
                        while partial_res.ind > queue.0 {
                            queue.0 = queue.1.pop_front().unwrap();
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
                        if e.1.len() == 0 {
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
                let mut ongoing_txns = state.ongoing_txs.write().await;
                let transaction = match ongoing_txns
                    .get_mut(&cid)
                    .and_then(|csn_map| csn_map.get_mut(&csn_num))
                {
                    Some(s) => s,
                    None => panic!("Missing appropriate entries in ongoing transactions"),
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
                    TxnStatus::NotStarted(_) => {
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
                        let map = HashMap::from(res.map);
                        (*transact_ent).result = Some(TxnRes { map });
                    }
                    (None, None) | (None, Some(_)) => (),
                };

                // backwards ack through chain if done
                if done {
                    let res = transact_ent.result.clone();
                    *transaction = TxnStatus::Done(TransactionEntry::new_res(
                        res.clone(),
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
            } else {
                panic!("ExecNotif channel sender closed");
            }
        }
    }

    pub(super) async fn proc_append(
        mut proc_ch: mpsc::Receiver<AppendTransactRequest>,
        state: Arc<ManagerNodeState>,
        node_state: Arc<NodeStatus>,
        cluster_conns: Arc<ChannelPool<u32>>,
    ) {
        let mut log = VecDeque::<AppendTransactRequest>::new();

        loop {
            if let Some(released_req) = proc_ch.recv().await {
                let ind = log.len();
                log.push_back(released_req.clone());
                let csn = released_req.csn.as_ref().unwrap();
                let addr = released_req.addr.clone();
                let mut ongoing_txns = state.ongoing_txs.write().await;
                match ongoing_txns.entry(csn.cid) {
                    Occupied(mut o) => {
                        if let Some(old_ent) = o
                            .get_mut()
                            .insert(csn.sn, TxnStatus::InProg(TransactionEntry::new(addr)))
                        {
                            if let TxnStatus::NotStarted(notif) = old_ent {
                                notif.notify_waiters();
                            }
                        }
                        o.get_mut().retain(|k, _| *k >= released_req.ack_bound);
                    }
                    Vacant(v) => {
                        v.insert(BTreeMap::from([(
                            csn.sn,
                            TxnStatus::InProg(TransactionEntry::new(addr)),
                        )]));
                    }
                };
                drop(ongoing_txns);

                match &*node_state {
                    NodeStatus::Head(succ) => {
                        update_view(&state, ind, csn.clone(), &released_req.txn).await;
                        let new_req = AppendTransactRequest {
                            ind: u64::try_from(ind).unwrap(),
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
                        let reqs =
                            update_view_tail(&state, ind, csn.clone(), released_req.txn).await;
                        for (k, v) in reqs.into_iter() {
                            tokio::spawn(send_cluster_rpc(
                                k,
                                RPCRequest::ExecAppend(v),
                                cluster_conns.clone(),
                            ));
                        }
                    }
                }
            } else {
                panic!("Processing channel sender closed")
            }
        }
    }

    // if head. can push into log in any order, else need to
    // do another round of serialization
    pub(super) async fn log_send_or_queue(
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

    pub(super) async fn scheduler(
        state: Arc<ManagerNodeState>,
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
                let csn = new_req.csn.as_ref().unwrap();
                match client_queue.entry(csn.cid) {
                    Occupied(mut o) => {
                        let (greatest_csn, queue) = o.get_mut();
                        if csn.sn == *greatest_csn + 1 {
                            // If no reordering, check for log reordering and service
                            Self::log_send_or_queue(
                                new_req,
                                &mut log_ind,
                                &mut log_queue,
                                &proc_ch,
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
                                    client_queue_head,
                                    &mut log_ind,
                                    &mut log_queue,
                                    &proc_ch,
                                    node_state.clone(),
                                )
                                .await;
                            }
                            *greatest_csn = curr_sn;
                        } else if csn.sn <= *greatest_csn {
                            // Have seen this sequence number before
                            // Check if we have a response ready
                            let ongoing_txns = state.ongoing_txs.read().await;
                            match (&*node_state, ongoing_txns.get(&csn.cid)) {
                                (NodeStatus::Head(_), Some(csn_map)) => {
                                    if let Some(status) = csn_map.get(&csn.sn) {
                                        if let TxnStatus::Done(txn_entry) = status {
                                            let resp = SessionRespWriteRequest {
                                                csn: Some(csn.clone()),
                                                res: txn_entry.result.clone(),
                                            };
                                            tokio::spawn(send_client_rpc(
                                                RPCRequest::SessResponseWrite(resp),
                                                new_req.addr,
                                            ));
                                        }
                                    }
                                }
                                _ => (),
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
                                new_req,
                                &mut log_ind,
                                &mut log_queue,
                                &proc_ch,
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
            } else {
                panic!("Append req channel closed");
            }
        }
    }
}
