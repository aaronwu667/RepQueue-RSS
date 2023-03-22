use std::{
    collections::{hash_map::Entry::*, BTreeMap, HashMap, VecDeque},
    sync::Arc,
};

use proto::{
    common_decls::{exec_notif_request::ReqStatus, Csn, ExecNotifRequest, TxnRes},
    manager_net::{AppendTransactRequest, ExecAppendTransactRequest},
};
use tokio::sync::mpsc;
use tonic::transport::Channel;

use super::TransactionService;
use crate::{
    rpc_utils::{send_chain_rpc, send_exec, RPCRequest},
    state_update::{update_view, update_view_tail},
    ManagerNodeState, NodeStatus, TxnStatus,
};

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
                let (Csn { cid, sn: csn_num }, shards) = match ind_to_sh.remove(&req.ind) {
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
                let mut ongoing_txns = state.ongoing_txs.lock().await;
                let status = match ongoing_txns
                    .get_mut(&cid)
                    .and_then(|csn_map| csn_map.get_mut(&csn_num))
                {
                    Some(s) => s,
                    None => panic!("Missing appropriate entries in ongoing transactions"),
                };
                *status = (TxnStatus::Done, req.res.clone());
                drop(ongoing_txns);

                // send result to client if head, otherwise continue backwards ack
                if let Some(ref ch) = pred {
                    send_chain_rpc(RPCRequest::ExecAppend(req), ch.clone()).await;
                } else {
                    // TODO SessionRespWrite
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
                let mut ongoing_txns = state.ongoing_txs.lock().await;
                let status = match ongoing_txns
                    .get_mut(&cid)
                    .and_then(|csn_map| csn_map.get_mut(&csn_num))
                {
                    Some(s) => s,
                    None => panic!("Missing appropriate entries in ongoing transactions"),
                };
                match (partial_res.res, status.1.as_mut()) {
                    (Some(res), Some(resp_data)) => {
                        for (k, v) in res.map {
                            resp_data.map.insert(k, v);
                        }
                    }
                    (Some(res), None) => {
                        let map = HashMap::from(res.map);
                        status.1 = Some(TxnRes { map });
                    }
                    (None, None) | (None, Some(_)) => (),
                };
                if done {
                    status.0 = TxnStatus::Done;
                    let req = ExecAppendTransactRequest {
                        ind: partial_res.ind,
                        res: status.1.clone(),
                    };
                    tokio::spawn(send_chain_rpc(RPCRequest::ExecAppend(req), pred.clone()));
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
    ) {
        let mut log = VecDeque::<AppendTransactRequest>::new();

        loop {
            if let Some(released_req) = proc_ch.recv().await {
                let ind = log.len();
                log.push_back(released_req.clone());
                let csn = released_req.csn.as_ref().unwrap();
                let mut ongoing_txns = state.ongoing_txs.lock().await;
                ongoing_txns
                    .entry(csn.cid)
                    .and_modify(|m| {
                        m.insert(csn.sn, (TxnStatus::InProg, None));
                        m.retain(|k, _| *k >= released_req.ack_bound)
                    })
                    .or_insert(BTreeMap::from([(csn.sn, (TxnStatus::InProg, None))]));
                drop(ongoing_txns);

                match &*node_state {
                    NodeStatus::Head(succ) => {
                        update_view(&state, ind, csn.clone(), &released_req.txn).await;
                        let new_req = AppendTransactRequest {
                            ind: u64::try_from(ind).unwrap(),
                            ..released_req
                        };
                        tokio::spawn(send_chain_rpc(RPCRequest::Append(new_req), succ.clone()));
                    }
                    NodeStatus::Middle(_, succ) => {
                        update_view(&state, ind, csn.clone(), &released_req.txn).await;
                        tokio::spawn(send_chain_rpc(
                            RPCRequest::Append(released_req),
                            succ.clone(),
                        ));
                    }
                    NodeStatus::Tail(_, cluster) => {
                        let reqs =
                            update_view_tail(&state, ind, csn.clone(), released_req.txn).await;
                        for (k, v) in reqs.into_iter() {
                            tokio::spawn(send_exec(k, v, cluster.clone()));
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
                let Csn { cid, sn: csn_num } = new_req.csn.as_ref().unwrap();
                match client_queue.entry(*cid) {
                    Occupied(mut o) => {
                        let (greatest_csn, queue) = o.get_mut();
                        if *csn_num == *greatest_csn + 1 {
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
                        } else if *csn_num <= *greatest_csn {
                            // Have seen this sequence number before
                            // Check if we have a response ready
                            let ongoing_txns = state.ongoing_txs.lock().await;
                            match (&*node_state, ongoing_txns.get(&cid)) {
                                (NodeStatus::Head(_), Some(csn_map)) => {
                                    if let Some(status_tup) = csn_map.get(csn_num) {
                                        if let TxnStatus::Done = status_tup.0 {
                                            // TODO SessionRespWrite
                                        }
                                    }
                                }
                                _ => (),
                            }
                        } else {
                            // Otherwise, put onto queue for waiting
                            queue.insert(*csn_num, new_req);
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
