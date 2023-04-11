use async_trait::async_trait;
use proto::{
    client_lib::{
        client_library_server::ClientLibrary, SessionRespReadRequest, SessionRespWriteRequest,
    },
    common_decls::{Csn, Empty, TransactionOp},
    manager_net::{
        manager_service_client::ManagerServiceClient, AppendTransactRequest,
        ReadOnlyTransactRequest,
    },
};
use rand::Rng;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};
use tokio::sync::{oneshot, Mutex, RwLock, Semaphore};
use tonic::{
    transport::{Channel, Endpoint},
    Request, Response, Status,
};

#[derive(Debug)]
struct ReadResult {
    num_shards: Option<u32>,
    data: HashMap<String, Option<String>>,
    // lsn: u64,
}

struct ClusterConnections {
    head: Channel,
    chain_node: Channel,
}

type CallbackChannel = oneshot::Sender<(u64, Option<HashMap<String, Option<String>>>)>;

// TODO (high priority): Client side retries, sessions
pub struct ClientSession {
    cid: u64,
    cwsn: Mutex<u64>,
    crsn: Mutex<u64>,
    max_concurrent: Arc<Semaphore>,
    rw_in_prog: Arc<RwLock<BTreeSet<u64>>>,
    // upper_bounds: Arc<Mutex<BTreeMap<u64, u64>>>,
    read_results: Arc<RwLock<HashMap<u64, Arc<Mutex<ReadResult>>>>>,
    read_callbacks: Arc<Mutex<HashMap<u64, CallbackChannel>>>,
    write_callbacks: Arc<Mutex<HashMap<u64, CallbackChannel>>>,
    cluster_conns: ClusterConnections,
    my_addr: String,
}

impl ClientSession {
    // TODO (Low priority): separate initialization of server from new struct
    // -> i.e. support sessions
    pub async fn new(
        max_concurrency: usize,
        my_addr: String,
        head: String,
        chain_node: String,
    ) -> (Self, ClientSessionServer) {
        let mut rng = rand::thread_rng();
        let head = match Endpoint::from_shared(head).unwrap().connect().await {
            Ok(chan) => chan,
            Err(e) => panic!("Error when connecting to chain head {}", e),
        };

        let chain_node = match Endpoint::from_shared(chain_node).unwrap().connect().await {
            Ok(chan) => chan,
            Err(e) => panic!("Error when connecting to chain node {}", e),
        };

        let rw_in_prog = Arc::new(RwLock::new(BTreeSet::new()));
        let read_results = Arc::new(RwLock::new(HashMap::new()));
        let read_callbacks = Arc::new(Mutex::new(HashMap::new()));
        let write_callbacks = Arc::new(Mutex::new(HashMap::new()));
        (
            Self {
                cid: rng.gen(),
                cwsn: Mutex::new(0),
                crsn: Mutex::new(0),
                max_concurrent: Arc::new(Semaphore::new(max_concurrency)),
                rw_in_prog: rw_in_prog.clone(),
                read_results: read_results.clone(),
                read_callbacks: read_callbacks.clone(),
                write_callbacks: write_callbacks.clone(),
                cluster_conns: ClusterConnections { head, chain_node },
                my_addr,
            },
            ClientSessionServer::new(rw_in_prog, read_results, read_callbacks, write_callbacks),
        )
    }

    pub async fn read_only_transaction(
        &self,
        txn: Vec<String>,
    ) -> (u64, Option<HashMap<String, Option<String>>>) {
        if txn.is_empty() {
            return (0, None);
        }

        let permit = self
            .max_concurrent
            .acquire()
            .await
            .expect("Semaphore acquisition error");

        let mut crsn = self.crsn.lock().await;
        *crsn += 1;
        let my_crsn = *crsn;
        drop(crsn);

        // write dependency
        let rw_in_prog = self.rw_in_prog.read().await;
        let write_dep = rw_in_prog.last().copied();
        drop(rw_in_prog);

        // add entry to read result
        let mut read_results = self.read_results.write().await;
        let read_result = ReadResult {
            num_shards: None,
            data: HashMap::new(),
        };
        read_results.insert(my_crsn, Arc::new(Mutex::new(read_result)));
        drop(read_results);

        // register callback channel
        let mut read_callbacks = self.read_callbacks.lock().await;
        let (send, recv) = oneshot::channel();
        read_callbacks.insert(my_crsn, send);
        drop(read_callbacks);

        // send to chain node
        let mut client = ManagerServiceClient::new(self.cluster_conns.chain_node.clone());
        let req = ReadOnlyTransactRequest {
            keys: txn,
            csn: Some(Csn {
                cid: self.cid,
                sn: my_crsn,
            }),
            write_dep,
            lsn_const: None,
            addr: self.my_addr.clone(),
        };

        if let Err(e) = client.read_only_transact(req).await {
            panic!("Sending to chain node failed {}", e)
        }

        // wait for result
        match recv.await {
            Ok(res) => {
                drop(permit);
                res
            }
            Err(e) => panic!("Receiving from callback failed {}", e),
        }
    }

    pub async fn read_write_transaction(
        &self,
        txn: HashMap<String, TransactionOp>,
    ) -> (u64, Option<HashMap<String, Option<String>>>) {
        if txn.is_empty() {
            return (0, None);
        }

        let permit = self
            .max_concurrent
            .acquire()
            .await
            .expect("Semaphore acquisition error");

        let mut cwsn = self.cwsn.lock().await;
        *cwsn += 1;
        let my_cwsn = *cwsn;
        drop(cwsn);

        let mut rw_in_prog = self.rw_in_prog.write().await;
        rw_in_prog.insert(my_cwsn);
        let ack_bound = *rw_in_prog.first().unwrap();
        drop(rw_in_prog);

        // register callback channel
        let mut write_callbacks = self.write_callbacks.lock().await;
        let (send, recv) = oneshot::channel();
        write_callbacks.insert(my_cwsn, send);
        drop(write_callbacks);

        // send to head
        let mut client = ManagerServiceClient::new(self.cluster_conns.head.clone());
        let req = AppendTransactRequest {
            txn,
            csn: Some(Csn {
                cid: self.cid,
                sn: my_cwsn,
            }),
            ack_bound,
            ind: 0,
            addr: self.my_addr.clone(),
        };

        if let Err(e) = client.append_transact(req).await {
            panic!("Sending to head failed {}", e)
        }

        // wait for result
        match recv.await {
            Ok(res) => {
                drop(permit);
                res
            }
            Err(e) => panic!("Receiving from callback failed {}", e),
        }
    }
}

// Listens to responses from cluster and invokes registered callbacks
pub struct ClientSessionServer {
    rw_in_prog: Arc<RwLock<BTreeSet<u64>>>,
    read_results: Arc<RwLock<HashMap<u64, Arc<Mutex<ReadResult>>>>>,
    read_callbacks: Arc<Mutex<HashMap<u64, CallbackChannel>>>,
    write_callbacks: Arc<Mutex<HashMap<u64, CallbackChannel>>>,
}

impl ClientSessionServer {
    fn new(
        rw_in_prog: Arc<RwLock<BTreeSet<u64>>>,
        read_results: Arc<RwLock<HashMap<u64, Arc<Mutex<ReadResult>>>>>,
        read_callbacks: Arc<Mutex<HashMap<u64, CallbackChannel>>>,
        write_callbacks: Arc<Mutex<HashMap<u64, CallbackChannel>>>,
    ) -> Self {
        Self {
            rw_in_prog,
            read_results,
            read_callbacks,
            write_callbacks,
        }
    }
}

#[async_trait]
impl ClientLibrary for ClientSessionServer {
    async fn session_resp_read(
        &self,
        request: Request<SessionRespReadRequest>,
    ) -> Result<Response<Empty>, Status> {
        // first get the result corresponding to sequence num
        let req = request.into_inner();
        let Csn { cid: _, sn } = req.csn.expect("Csn should not be empty");
        let read_results = self.read_results.read().await;
        let partial_res_guard = match read_results.get(&sn) {
            Some(mutex) => mutex.clone(),
            None => panic!("Returned read result does not have entry"),
        };
        drop(read_results);

        // update the entry
        let mut partial_res = partial_res_guard.lock().await;
        let curr_num = match partial_res.num_shards {
            Some(num) => {
                assert!(num != 0);
                let new_num = num - 1;
                partial_res.num_shards = Some(new_num);
                new_num
            }
            None => {
                let num = req.num_shards - 1;
                partial_res.num_shards = Some(num);
                num
            }
        };
        for (k, v) in req.res.into_iter() {
            partial_res.data.insert(k, v.value);
        }
        drop(partial_res);

        // remove from the map if we have data from all shards
        if curr_num == 0 {
            // need to relock here
            let mut read_results = self.read_results.write().await;
            let complete_res_guard_opt = read_results.remove(&sn);
            drop(read_results);

            if let Some(complete_res_guard) = complete_res_guard_opt {
                let complete_res = complete_res_guard.lock().await;
                // send result on channel
                let mut read_callbacks = self.read_callbacks.lock().await;
                let ch = match read_callbacks.remove(&sn) {
                    Some(cb) => cb,
                    None => panic!("No callback registered for read result"),
                };
                drop(read_callbacks);

                if complete_res.data.is_empty() {
                    ch.send((sn, None)).expect("Read receiver dropped");
                } else {
                    ch.send((sn, Some(complete_res.data.clone()))).expect("Read receiver dropped");
                }                
            }
        }
        Ok(Response::new(Empty {}))
    }

    async fn session_resp_write(
        &self,
        request: Request<SessionRespWriteRequest>,
    ) -> Result<Response<Empty>, Status> {
        let req = request.into_inner();
        let Csn { cid: _, sn } = req.csn.expect("Csn should not be empty");
        let mut rw_in_prog = self.rw_in_prog.write().await;
        rw_in_prog.remove(&sn);
        drop(rw_in_prog);

        // send result on channel
        let mut write_callbacks = self.write_callbacks.lock().await;
        let ch = match write_callbacks.remove(&sn) {
            Some(cb) => cb,
            None => panic!("No callback registered for result"),
        };
        drop(write_callbacks);

        if ch
            .send((
                sn,
                req.res
                    .map(|txn_res| txn_res.map.into_iter().map(|(k, v)| (k, v.value)).collect()),
            ))
            .is_err()
        {
            panic!("Read receiver dropped")
        }

        Ok(Response::new(Empty {}))
    }
}
