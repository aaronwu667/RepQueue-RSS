use std::collections::{btree_map::Entry::*, BTreeMap, HashMap};
use std::sync::Arc;

use crate::channel_pool::ChannelPool;
use crate::utils::add_remote_deps;
use crate::version_store::MemStore;
use crate::{RaftRepl, StoreRequest, TAIL_NID};
use async_trait::async_trait;
use openraft::error::ClientWriteError;
use openraft::NodeId;
use proto::client_lib::client_library_client::ClientLibraryClient;
use proto::client_lib::SessionRespReadRequest;
use proto::common_decls::exec_notif_request::ReqStatus;
use proto::common_decls::{transaction_op::Op::*, Empty};
use proto::common_decls::{ExecNotifInner, ExecNotifRequest};
use proto::common_decls::{TxnRes, ValueField};
use proto::manager_net::manager_service_client::ManagerServiceClient;
use proto::shard_net::shard_service_client::ShardServiceClient;
use proto::shard_net::{
    shard_service_server::ShardService, ExecAppendRequest, ExecReadRequest, PutReadRequest,
};
use tokio::sync::{mpsc, oneshot, watch};
use tonic::{Request, Response, Status};

const DEBUG: bool = true;

struct WriteQueueEntry {
    req: ExecAppendRequest,
    ch: Option<oneshot::Receiver<HashMap<String, ValueField>>>,
}

struct DepResolveEntry {
    ind: u64,                                         // Transaction manager index
    num_deps: u32,                                    // number of dependencies expected
    ch: oneshot::Sender<HashMap<String, ValueField>>, // channel to communicate with executor
}

// Raft-based shard group service
// TODO (BUG): Include shardId and change RPC responses according to proto change
pub struct ShardServer {
    // TODO (med priority): separate connection pool for transaction manager nodes
    // Since no failures for experiments, acceptable not to have this for now
    connections: Arc<ChannelPool<NodeId>>,
    store: Arc<MemStore>,
    shard_id: u32,

    // Appends
    enqueue_reqs: mpsc::Sender<WriteQueueEntry>,
    handle_deps: mpsc::Sender<PutReadRequest>,
    dep_resolv_ch: mpsc::Sender<DepResolveEntry>,

    // Read-only
    enqueue_reads: mpsc::Sender<ExecReadRequest>,
    // TODO (low priority): can implement some form of congestion feedback via shared len var
}

impl ShardServer {
    pub fn new(
        r: Arc<RaftRepl>,
        c: Arc<ChannelPool<NodeId>>,
        m: Arc<MemStore>,
        shard_id: u32,
        wn: watch::Receiver<u64>,
        rn: watch::Receiver<u64>,
    ) -> Self {
        let (enq_tx, enq_rx) = mpsc::channel::<WriteQueueEntry>(1000);
        let (dep_resolv_tx, dep_resolv_rx) = mpsc::channel::<DepResolveEntry>(1000);
        let (handle_dep_tx, handle_dep_rx) = mpsc::channel::<PutReadRequest>(1000);
        let (handle_read_tx, handle_read_rx) = mpsc::channel::<ExecReadRequest>(1000);
        let ret = Self {
            connections: c.clone(),
            enqueue_reqs: enq_tx,
            dep_resolv_ch: dep_resolv_tx,
            handle_deps: handle_dep_tx,
            enqueue_reads: handle_read_tx,
            store: m.clone(),
            shard_id,
        };
        tokio::spawn(Self::proc_write_queue(r, c, enq_rx, wn, shard_id));
        tokio::spawn(Self::dep_resolver(handle_dep_rx, dep_resolv_rx, m.clone()));
        tokio::spawn(Self::proc_read_queue(handle_read_rx, rn, m));
        ret
    }

    async fn serve_remote(
        remote_id: u64,
        ind: u64,
        vals: HashMap<String, ValueField>,
        connections: Arc<ChannelPool<NodeId>>,
    ) {
        let mut client = connections
            .get_client(ShardServiceClient::new, remote_id)
            .await;
        if let Err(e) = client.put_read(PutReadRequest { ind, res: vals }).await {
            eprintln!("Serving remote read failed: {}", e);
        }
    }

    async fn serve_tail(resp: ExecNotifRequest, connections: Arc<ChannelPool<NodeId>>) {
        if !DEBUG {
            tokio::spawn({
                let conns = connections;
                async move {
                    let mut client = conns.get_client(ManagerServiceClient::new, TAIL_NID).await;
                    if let Err(e) = client.exec_notif(resp).await {
                        eprintln!("Error when sending to manager node {}", e);
                    }
                }
            });
        } else {
            println!("{:?}", resp);
        }
    }

    async fn serve_read(ent: ExecReadRequest, store: Arc<MemStore>) {
        let state = store.state.read().await;
        let mut res_map = HashMap::<String, ValueField>::new();
        for read_key in ent.txn.into_iter() {
            let read_res = state.get(&read_key, &ent.fence);
            res_map.insert(read_key, ValueField::new(read_res));
        }
        let resp = SessionRespReadRequest::new(res_map, ent.csn, ent.fence, ent.num_shards);
        if !DEBUG {
            tokio::spawn(async move {
                let client = ClientLibraryClient::connect(ent.addr).await;
                match client {
                    Ok(mut c) => {
                        if let Err(e) = c.session_resp_read(resp).await {
                            eprintln!("Error when sending to client: {}", e);
                        }
                    }
                    Err(e) => eprintln!("Error when connecting to client: {}", e),
                }
            });
        } else {
            println!("{:?}", resp.res);
        }
    }

    async fn executor(
        mut ent: WriteQueueEntry,
        raft: Arc<RaftRepl>,
        connections: Arc<ChannelPool<NodeId>>,
        shard_id: u32,
    ) {
        // resolve local dependencies, if any
        if let Some(ch) = ent.ch {
            // don't do anything with this at the moment
            // TODO (low priority): Support conditional evaluation
            match ch.await {
                Ok(_) => (),
                Err(_) => eprintln!("Timeout, Dep resolver dropped the channel"),
            }
        }

        let resp = raft.client_write(StoreRequest::from(&ent.req)).await;
        match resp {
            Ok(resp) => {
                let read_res = &resp.data.res;

                // if there are reads, we need to handle remote dependencies
                if let Some(read_res) = read_res {
                    let mut remote_deps = HashMap::<u64, HashMap<String, ValueField>>::new();

                    // build
                    for (k, v) in read_res.iter() {
                        let remote_groups = match ent.req.txn.remove(k) {
                            Some(txn_op) => txn_op.serve_remote_groups,
                            None => panic!("Key processed not present in original request"),
                        };
                        add_remote_deps(
                            k.to_owned(),
                            v.to_owned(),
                            &mut remote_deps,
                            remote_groups,
                        );
                    }

                    // serve
                    for (remote_id, vals) in remote_deps.into_iter() {
                        tokio::spawn(Self::serve_remote(
                            remote_id,
                            ent.req.ind,
                            vals,
                            connections.clone(),
                        ));
                    }
                }

                //Exec notif on transaction manager
                let resp = ExecNotifRequest {
                    req_status: Some(ReqStatus::Response(ExecNotifInner {
                        res: Option::<TxnRes>::from(resp.data),
                        shard_id,
                        ind: ent.req.ind,
                    })),
                };
                Self::serve_tail(resp, connections).await;
            }
            Err(err) => match err {
                ClientWriteError::ForwardToLeader(_) => {
                    let resp = ExecNotifRequest {
                        req_status: Some(ReqStatus::WrongLeader(true)),
                    };
                    Self::serve_tail(resp, connections).await;
                }
                _ => eprintln!("Raft group error"),
            },
        }
    }

    // Task that services write queue
    async fn proc_write_queue(
        raft: Arc<RaftRepl>,
        connections: Arc<ChannelPool<NodeId>>,
        mut req_ch: mpsc::Receiver<WriteQueueEntry>,
        mut ssn_watch: watch::Receiver<u64>,
        shard_id: u32,
    ) {
        let mut write_queue: BTreeMap<u64, WriteQueueEntry> = BTreeMap::new(); // ssn |-> (request details, dep channel)
        let mut curr_ssn: u64 = 0;

        loop {
            let mut new_req: Option<WriteQueueEntry> = None;
            tokio::select! {
                _ = ssn_watch.changed() => {curr_ssn = *ssn_watch.borrow()},
                r = req_ch.recv() => { new_req = Some(r.expect("Enqueue channel closed"));}
            };

            if let Some(ent) = new_req {
                if ent.req.sn == curr_ssn + 1 {
                    //println!("Launching executor for entry with ssn {}", ent.req.sn);
                    // launch executor
                    tokio::spawn(Self::executor(
                        ent,
                        raft.clone(),
                        connections.clone(),
                        shard_id,
                    ));
                } else {
                    //println!("Inserting entry with ssn {}", ent.req.sn);
                    write_queue.insert(ent.req.sn, ent);
                }
            } else if let Some(head) = write_queue.keys().next().copied() {
                // see if anything on the queue can be processed
                if head == curr_ssn + 1 {
                    // remove from queue and launch executor
                    let ent = write_queue.remove(&head).unwrap();
                    tokio::spawn(Self::executor(
                        ent,
                        raft.clone(),
                        connections.clone(),
                        shard_id,
                    ));
                }
            }
        }
    }

    // Task for aggregating local dependencies
    // notifies associated execution threads all dependencies satisfied
    async fn dep_resolver(
        mut recv_dep_ch: mpsc::Receiver<PutReadRequest>,
        mut resolv_dep_ch: mpsc::Receiver<DepResolveEntry>,
        store: Arc<MemStore>,
    ) {
        struct DepInfo {
            num_deps: u32,
            ch: oneshot::Sender<HashMap<String, ValueField>>,
        }

        // hash on txn manager index
        let mut dep_buf = BTreeMap::<u64, HashMap<String, ValueField>>::new();

        // working around ownership issues
        let mut metadata = HashMap::<u64, Option<DepInfo>>::new();

        loop {
            tokio::select! {
                resolve = resolv_dep_ch.recv() => {
                    let resolve = resolve.expect("Resolve channel closed");
                    // Could do a consistency check here on new vs old num_deps
                    match dep_buf.get(&resolve.ind) {
                        Some(buf_res) => {
                            if u32::try_from(buf_res.len()).unwrap() == resolve.num_deps {
                                if resolve.ch.send(buf_res.clone()).is_err() {
                                    eprintln!("Error while sending dependencies")
                                }
                            } else {
                                metadata.insert(resolve.ind, Some(DepInfo{num_deps: resolve.num_deps, ch: resolve.ch}));
                            }
                        }
                        None => {metadata.insert(resolve.ind, Some(DepInfo{num_deps: resolve.num_deps, ch: resolve.ch}));}
                    }
                },
                new_dep = recv_dep_ch.recv() => {
                    let new_dep = new_dep.expect("New dependency channel closed");
                    match dep_buf.entry(new_dep.ind) {
                        Occupied(mut o) => {
                            let map = o.get_mut();
                            for (k, v) in new_dep.res.into_iter() {
                                map.insert(k, v);
                            }

                            // send if we have gathered all dependencies
                            if let Some(Some(meta)) = metadata.get(&new_dep.ind) {
                                if meta.num_deps == u32::try_from(map.len()).unwrap() {
                                    let dep_info = metadata.remove(&new_dep.ind).flatten().unwrap();
                                    if dep_info.ch.send(map.clone()).is_err() {
                                        eprintln!("Error while sending dependencies")
                                    }
                                }
                            }
                        },
                        Vacant(v) => {
                            let mut map = HashMap::new();
                            for (k, v) in new_dep.res.into_iter() {
                                map.insert(k, v);
                            }

                            // send if we have gathered all dependencies
                            if let Some(Some(meta)) = metadata.get(&new_dep.ind) {
                                if meta.num_deps == u32::try_from(map.len()).unwrap() {
                                    let dep_info = metadata.remove(&new_dep.ind).flatten().unwrap();
                                    if dep_info.ch.send(map.clone()).is_err() {
                                        eprintln!("Error while sending dependencies")
                                    }
                                }
                            }
                            v.insert(map);
                        }
                    }
                }
            }

            // cleanup anything that's already committed
            // TODO (low priority): flush timeout on the select statement
            let state = store.state.read().await;
            dep_buf.retain(|k, _| *k > state.sh_exec);

            // drop lock before next loop iteration
            drop(state);
        }
    }

    // Task that serves reads, does NOT depend on leader status
    async fn proc_read_queue(
        mut req_ch: mpsc::Receiver<ExecReadRequest>,
        mut sh_exec_watch: watch::Receiver<u64>,
        store: Arc<MemStore>,
    ) -> ! {
        // TODO (low priority): client channel pool
        // fence |-> read request
        let mut read_queue = BTreeMap::<u64, ExecReadRequest>::new();
        let mut curr_sh_exec: u64 = 0;

        loop {
            tokio::select! {
                _ = sh_exec_watch.changed() => {
                    let mut keys_exec = Vec::with_capacity(read_queue.len() / 3);
                    curr_sh_exec = *sh_exec_watch.borrow();
                    for k in read_queue.keys() {
                        if *k <= curr_sh_exec {
                            keys_exec.push(*k);
                        }
                    }

                    // execute all eligible reads
                    for k in keys_exec.into_iter() {
                        let ent = read_queue.remove(&k).unwrap();
                        Self::serve_read(ent, store.clone()).await;
                    }
                },
                new_req = req_ch.recv() => {
                    let new_req = new_req.expect("Enqueue read requests channel closed");
                    if new_req.fence <= curr_sh_exec {
                        Self::serve_read(new_req, store.clone()).await;
                    } else {
                        read_queue.insert(new_req.fence, new_req);
                    }
                }
            }
        }
    }
}

#[async_trait]
impl ShardService for ShardServer {
    async fn shard_exec_append(
        &self,
        request: Request<ExecAppendRequest>,
    ) -> Result<Response<ExecNotifRequest>, Status> {
        let req = request.into_inner();
        let state = self.store.state.read().await;
        if req.sn <= state.ssn {
            // ack got lost in transit, since already committed, can just look at
            // memstore directly for reads
            let mut res = TxnRes {
                map: HashMap::new(),
            };
            let mut remote_deps = HashMap::<u64, HashMap<String, ValueField>>::new();
            for (k, v) in req.txn.into_iter() {
                if let Some(Read(_)) = v.op {
                    // gather read results
                    let mut read_val = None;
                    if let Some(s) = state.get(&k, &req.ind) {
                        read_val = Some(s.to_owned());
                    }
                    res.map
                        .insert(k.to_owned(), ValueField::new(read_val.to_owned()));

                    // build remote dependencies
                    add_remote_deps(k, read_val, &mut remote_deps, v.serve_remote_groups);
                }
            }

            // serve remote reads again
            for (remote_id, vals) in remote_deps.into_iter() {
                tokio::spawn(Self::serve_remote(
                    remote_id,
                    req.ind,
                    vals,
                    self.connections.clone(),
                ));
            }

            // don't want to send empty map
            let mut res_opt = None;
            if !res.map.is_empty() {
                res_opt = Some(res);
            }

            return Ok(Response::new(ExecNotifRequest {
                req_status: Some(ReqStatus::Response(ExecNotifInner {
                    res: res_opt,
                    shard_id: self.shard_id,
                    ind: req.ind,
                })),
            }));
        } else {
            // send request to queue servicer and give empty response as "promise"
            let mut dep_ch = None;
            if let Some(ref local_deps) = req.local_deps {
                let (send, recv) = oneshot::channel::<HashMap<String, ValueField>>();
                dep_ch = Some(recv);
                // WARNING: will fail when num of dep keys > sizeof(u32)
                if self
                    .dep_resolv_ch
                    .send(DepResolveEntry {
                        ind: req.ind,
                        num_deps: u32::try_from(local_deps.keys.len()).unwrap(),
                        ch: send,
                    })
                    .await
                    .is_err()
                {
                    drop(state);
                    panic!("dep_notif receiver dropped");
                }
            }

            if self
                .enqueue_reqs
                .send(WriteQueueEntry { req, ch: dep_ch })
                .await
                .is_err()
            {
                drop(state);
                panic!("enqueue_reqs receiver dropped");
            }

            return Ok(Response::new(ExecNotifRequest {
                req_status: Some(ReqStatus::Promise(true)),
            }));
        }
    }

    async fn shard_exec_read(
        &self,
        request: Request<ExecReadRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        if self.enqueue_reads.send(req).await.is_err() {
            panic!("enqueue_reads receiver closed or dropped");
        }

        return Ok(Response::new(Empty {}));
    }

    async fn put_read(&self, request: Request<PutReadRequest>) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        if self.handle_deps.send(req).await.is_err() {
            panic!("handle_deps receiver closed or dropped");
        }

        return Ok(Response::new(Empty {}));
    }
}
