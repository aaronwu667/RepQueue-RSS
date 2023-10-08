use client_library::{client_session::ClientSession, new_op, Op, TransactionOp};
use proto::client_lib::client_library_server::ClientLibraryServer;
use rand::prelude::Distribution;
use serde::Deserialize;
use std::{
    collections::{BTreeSet, HashMap},
    env,
    fs::{self, File},
    io::Write,
    sync::Arc,
};
use tonic::transport::Server;

#[derive(Deserialize, Debug)]
struct Conf {
    skew: f64,
    num_keys: usize,
    my_addr: String, // no http
    head_addr: String,
    chain_addr: String,
    results_path: String,
    client_machine: u32,
    client_proc: u32,
    experiment_num: u32,
    client_concurrency: usize,
}

enum TxnType {
    RO(Vec<String>),
    RW(HashMap<String, TransactionOp>),
}

fn get_uri(host: &str) -> String {
    let mut uri = "http://".to_owned();
    uri.push_str(host);
    uri
}

#[tokio::main]
async fn main() {
    let mut args: Vec<String> = env::args().skip(1).collect();
    let conf_file_path = args.pop().unwrap();
    let conf_file = fs::read_to_string(conf_file_path).expect("No file found at provided path");
    let conf_values: Conf =
        serde_json::from_str(conf_file.as_ref()).expect("Unable to parse config file");

    let my_serv_addr = get_uri(&conf_values.my_addr);
    let head_addr = get_uri(&conf_values.head_addr);
    let chain_addr = get_uri(&conf_values.chain_addr);

    let (client_lib, server) = ClientSession::new(
        conf_values.client_concurrency,
        my_serv_addr,
        head_addr,
        chain_addr,
    )
    .await;
    let client_lib = Arc::new(client_lib);
    // start client server in another task
    tokio::spawn(async move {
        if Server::builder()
            .add_service(ClientLibraryServer::new(server))
            .serve(conf_values.my_addr.parse().unwrap())
            .await
            .is_err()
        {
            panic!("Client library server failed")
        }
    });

    // Create transactions for experiment
    let zipf = zipf::ZipfDistribution::new(conf_values.num_keys, conf_values.skew).unwrap();
    let mut rng = rand::thread_rng();
    let num_txns = 1010; // 10 transactions to warm up
    let mut txns: Vec<HashMap<String, TransactionOp>> = Vec::with_capacity(num_txns);
    for _ in 0..num_txns {
        let num_keys = 10;
        let mut txn = HashMap::new();
        for _ in 0..num_keys {
            let key = zipf.sample(&mut rng).to_string();
            let value = zipf.sample(&mut rng).to_string(); // value doesn't really matter
            txn.insert(key, new_op(Op::Write(value)));
        }
        txns.push(txn);
    }
    /*
        // start experiment and collect metrics
        let num_concurrent = conf_values.client_concurrency;
        let mut futs = Vec::with_capacity(num_concurrent);
        let mut res = BTreeSet::new();
        for txn in txns.into_iter() {
            if futs.len() == num_concurrent {
                let fut_res: Vec<Result<(u64, u128), _>> = futures::future::join_all(futs).await;
                for e in fut_res.into_iter() {
                    res.insert(e.unwrap());
                }
                futs = Vec::with_capacity(num_concurrent);
            }
            futs.push(tokio::spawn({
                let client_lib = client_lib.clone();
                async move {
                    let start = time::Instant::now();
                    let (cwsn, _) = client_lib.read_write_transaction(txn).await;
                    (cwsn, start.elapsed().as_millis())
                }
            }));
        }
        for e in futures::future::join_all(futs).await.into_iter() {
            res.insert(e.unwrap());
    }
        */

    let futs = txns.into_iter().map(|req| {
        tokio::spawn({
            let client_lib = client_lib.clone();
            async move {
                let (cwsn, dur, _) = client_lib.read_write_transaction(req).await;
                (cwsn, dur.as_millis())
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

    let base_path = format!(
        "{}/out_{}",
        conf_values.results_path, conf_values.experiment_num
    );
    fs::create_dir_all(base_path.clone()).unwrap();

    // write latencies to disk
    let mut write_only_path = base_path.clone();
    write_only_path.push_str(&format!(
        "/WO_client_{}-{}.log",
        conf_values.client_machine, conf_values.client_proc
    ));
    let mut out = File::create(write_only_path).expect("File failed to open");
    out.write_all(res.join("\n").as_bytes()).unwrap();

    // start mixed read-only and read-write workload
    let zipf = zipf::ZipfDistribution::new(conf_values.num_keys, conf_values.skew).unwrap();
    let mut rng = rand::thread_rng();
    let num_txns = 700;
    let mut txns: Vec<TxnType> = Vec::with_capacity(num_txns);
    for i in 0..num_txns {
        let num_keys = 10;
        if i % 10 == 0 && i != 0 {
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
    /*
    let mut futs = Vec::with_capacity(num_concurrent);
    let mut res = BTreeSet::new();
    for (i, req) in txns.into_iter().enumerate() {
        if futs.len() == num_concurrent {
            let fut_res: Vec<Result<(usize, u128), _>> = futures::future::join_all(futs).await;
            for e in fut_res.into_iter() {
                res.insert(e.unwrap());
            }
            futs = Vec::with_capacity(num_concurrent);
        }
        match req {
            TxnType::RW(req) => futs.push(tokio::spawn({
                let client_lib = client_lib.clone();
                async move {
                    let start = time::Instant::now();
                    client_lib.read_write_transaction(req).await;
                    (i, start.elapsed().as_millis())
                }
            })),
            TxnType::RO(req) => futs.push(tokio::spawn({
                let client_lib = client_lib.clone();
                async move {
                    let start = time::Instant::now();
                    client_lib.read_only_transaction(req).await;
                    (i, start.elapsed().as_millis())
                }
            })),
        }
    }
    for e in futures::future::join_all(futs).await.into_iter() {
        res.insert(e.unwrap());
    }
    */

    let futs = txns.into_iter().enumerate().map(|(i, req)| match req {
        TxnType::RW(req) => tokio::spawn({
            let client_lib = client_lib.clone();
            async move {
                let (_, dur, _) = client_lib.read_write_transaction(req).await;
                (i, dur.as_millis())
            }
        }),
        TxnType::RO(req) => tokio::spawn({
            let client_lib = client_lib.clone();
            async move {
                let (_, dur, _) = client_lib.read_only_transaction(req).await;
                (i, dur.as_millis())
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

    // write latencies to disk
    let mut mixed_path = base_path;
    mixed_path.push_str(&format!(
        "/mix_client_{}-{}.log",
        conf_values.client_machine, conf_values.client_proc
    ));
    let mut out = File::create(mixed_path).expect("File failed to open");
    out.write_all(res.join("\n").as_bytes()).unwrap();
}
