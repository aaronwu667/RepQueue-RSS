use client_library::{client_session::ClientSession, new_op, Op, TransactionOp};
use proto::client_lib::client_library_server::ClientLibraryServer;
use rand::prelude::Distribution;
use std::{
    collections::{BTreeSet, HashMap},
    env,
    fs::File,
    io::Write,
    sync::Arc,
    time,
};
use tonic::transport::Server;

enum TxnType {
    RO(Vec<String>),
    RW(HashMap<String, TransactionOp>),
}

#[tokio::main]
async fn main() {
    let mut args: Vec<String> = env::args().skip(1).collect();
    let skew: f64 = args.pop().unwrap().parse().unwrap();
    let num_keys: usize = args.pop().unwrap().parse().unwrap();
    let my_addr = args.pop().unwrap();
    let head_addr = args.pop().unwrap();
    let chain_node = args.pop().unwrap();
    let mut my_serv_addr = "http://".to_owned();
    my_serv_addr.push_str(&my_addr);
    let (client_lib, server) = ClientSession::new(my_serv_addr, head_addr, chain_node).await;
    let client_lib = Arc::new(client_lib);
    // start client server in another task
    tokio::spawn(async move {
        if Server::builder()
            .add_service(ClientLibraryServer::new(server))
            .serve(my_addr.parse().unwrap())
            .await
            .is_err()
        {
            panic!("Client library server failed")
        }
    });

    // Create transactions for experiment
    let zipf = zipf::ZipfDistribution::new(num_keys, skew).unwrap();
    let mut rng = rand::thread_rng();
    let num_txns = 500;
    let mut txns: Vec<HashMap<String, TransactionOp>> = Vec::with_capacity(num_txns);
    for _ in 0..num_txns {
        let num_keys = 10; // should probably ask about this
        let mut txn = HashMap::new();
        for _ in 0..num_keys {
            let key = zipf.sample(&mut rng).to_string();
            let value = zipf.sample(&mut rng).to_string(); // value doesn't really matter
            txn.insert(key, new_op(Op::Write(value)));
        }
        txns.push(txn);
    }

    // start experiment and collect metrics
    let futs = txns.into_iter().map(|req| {
        tokio::spawn({
            let client_lib = client_lib.clone();
            async move {
                let start = time::Instant::now();
                let (cwsn, _) = client_lib.read_write_transaction(req).await;
                (cwsn, start.elapsed().as_millis())
            }
        })
    });

    let res: BTreeSet<_> = futures::future::join_all(futs)
        .await
        .into_iter()
        .map(|e| e.unwrap())
        .collect();

    let res: Vec<_> = res
        .into_iter()
        .map(|e| format!("{}, {}", e.0, e.1))
        .collect();

    // flush latencies to disk
    let mut out = File::create("/home/aaron/md-rss/test_output/write_only_latency.log")
        .expect("File failed to open");
    out.write_all(res.join("\n").as_bytes()).unwrap();

    // start mixed read-only and read-write workload
    let zipf = zipf::ZipfDistribution::new(num_keys, skew).unwrap();
    let mut rng = rand::thread_rng();
    let num_txns = 500;
    let mut txns: Vec<TxnType> = Vec::with_capacity(num_txns);
    for i in 0..num_txns {
        let num_keys = 10; // should probably ask about this
        if i % 10 == 0 {
            let mut txn = HashMap::new();
            for _ in 0..num_keys {
                let key = zipf.sample(&mut rng).to_string();
                let value = zipf.sample(&mut rng).to_string();
                txn.insert(key, new_op(Op::Write(value)));
            }
            txns.push(TxnType::RW(txn));
        } else {
            let mut txn = Vec::new();
            for _ in 0..num_keys {
                let key = zipf.sample(&mut rng).to_string();
                txn.push(key);
            }
            txns.push(TxnType::RO(txn));
        }
    }

    let futs = txns.into_iter().enumerate().map(|(i, req)| match req {
        TxnType::RW(req) => tokio::spawn({
            let client_lib = client_lib.clone();
            async move {
                let start = time::Instant::now();
                client_lib.read_write_transaction(req).await;
                (i, start.elapsed().as_millis())
            }
        }),
        TxnType::RO(req) => tokio::spawn({
            let client_lib = client_lib.clone();
            async move {
                let start = time::Instant::now();
                client_lib.read_only_transaction(req).await;
                (i, start.elapsed().as_millis())
            }
        }),
    });

    let res: BTreeSet<_> = futures::future::join_all(futs)
        .await
        .into_iter()
        .map(|e| e.unwrap())
        .collect();

    let res: Vec<_> = res
        .into_iter()
        .map(|e| format!("{}, {}", e.0 + 1, e.1))
        .collect();

    // flush latencies to disk
    let mut out = File::create("/home/aaron/md-rss/test_output/mixed_workload_latency.log")
        .expect("File failed to open");
    out.write_all(res.join("\n").as_bytes()).unwrap();
}
