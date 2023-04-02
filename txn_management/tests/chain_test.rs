use rand::prelude::*;
use std::{collections::HashMap, env, time::Duration};

use proto::{
    common_decls::{
        exec_notif_request::ReqStatus, transaction_op, Csn, ExecNotifInner, ExecNotifRequest,
        TransactionOp, TxnRes, ValueField,
    },
    manager_net::{
        manager_service_client::ManagerServiceClient, AppendTransactRequest,
        ReadOnlyTransactRequest,
    },
};
use tonic::transport::{Channel, Endpoint};

async fn send_read_write(cwsn: u64, mut client: ManagerServiceClient<Channel>) {
    let mut val = "Hello world ".to_owned();
    val.push_str(&cwsn.to_string());
    let mut txn = HashMap::<String, TransactionOp>::new();
    txn.insert(
        1.to_string(),
        TransactionOp {
            serve_remote_groups: vec![],
            op: Some(transaction_op::Op::Write(val.clone())),
        },
    );
    txn.insert(
        3.to_string(),
        TransactionOp {
            serve_remote_groups: vec![],
            op: Some(transaction_op::Op::Write(val)),
        },
    );
    txn.insert(
        5.to_string(),
        TransactionOp {
            serve_remote_groups: vec![],
            op: Some(transaction_op::Op::Read("".to_owned())),
        },
    );
    let req = AppendTransactRequest {
        txn,
        csn: Some(Csn { cid: 0, sn: cwsn }),
        ack_bound: 0,
        //ack_bound: cwsn - 1, <- recomment this back in for basic tail test
        ind: 0,
        addr: "Test client".to_owned(),
    };
    if let Err(e) = client.append_transact(req).await {
        panic!("Error {}", e)
    }
}

async fn send_read_only(crsn: u64, dep: Option<u64>, mut client: ManagerServiceClient<Channel>) {
    let req = ReadOnlyTransactRequest {
        keys: vec![1.to_string(), 3.to_string()],
        csn: Some(Csn { cid: 0, sn: crsn }),
        write_dep: dep,
        lsn_const: None,
        addr: "Test client read".to_owned(),
    };
    if let Err(e) = client.read_only_transact(req).await {
        panic!("Error {}", e)
    }
}

async fn send_cluster_ack(ind: u64, mut client: ManagerServiceClient<Channel>) {
    let req = ExecNotifRequest {
        req_status: Some(ReqStatus::Response(ExecNotifInner {
            res: Some(TxnRes {
                map: HashMap::from([(
                    5.to_string(),
                    ValueField {
                        value: Some(format!("Read result {}", ind)),
                    },
                )]),
            }),
            shard_id: 0,
            ind,
        })),
    };
    if let Err(e) = client.exec_notif(req).await {
        panic!("Error {}", e)
    }
}

#[tokio::test]
async fn basic_tail_test() {
    let mut args: Vec<String> = env::args().skip(1).collect();
    let tail_addr = args.pop().unwrap();
    let head_addr = args.pop().unwrap();
    let head_handle = Endpoint::from_shared(head_addr)
        .unwrap()
        .connect()
        .await
        .unwrap();
    
    let tail_handle = Endpoint::from_shared(tail_addr)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let head_client = ManagerServiceClient::new(head_handle);    
    let tail_client = ManagerServiceClient::new(tail_handle);

    // send transaction through chain
    send_read_write(1, head_client.clone()).await;

    // send ack separately
    send_cluster_ack(1, tail_client.clone()).await;

    // check that fences have been incremented properly (to 1)
    send_read_only(1, Some(1), head_client.clone()).await;
    send_read_only(1, None, tail_client.clone()).await;
    
    // send another transaction (that is not acked)
    send_read_write(2, head_client.clone()).await;   
    
    // check that reads that take dependency on 2 have appropriate fence
    send_read_only(2, Some(1), head_client.clone()).await;
    send_read_only(2, None, tail_client.clone()).await;

    // check that out of order reads get assigned fences properly
    send_read_write(3, head_client.clone()).await;

    send_read_only(4, Some(2), tail_client.clone()).await;
    send_cluster_ack(2, tail_client.clone()).await;
    send_cluster_ack(3, tail_client.clone()).await;
    send_read_only(3, Some(2), tail_client.clone()).await;
    send_read_only(5, Some(2), tail_client.clone()).await;

    // check that we're cleaning up values appropriately
    send_read_write(4, head_client.clone()).await;
    send_read_only(6, Some(3), tail_client.clone()).await;
}

#[tokio::test]
async fn chain_test() {
    // get handle to head and tail
    let mut args: Vec<String> = env::args().skip(1).collect();
    let tail_addr = args.pop().unwrap();
    let head_addr = args.pop().unwrap();

    let tail_handle = Endpoint::from_shared(tail_addr)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let head_handle = Endpoint::from_shared(head_addr)
        .unwrap()
        .connect()
        .await
        .unwrap();

    let head_client = ManagerServiceClient::new(head_handle);
    let tail_client = ManagerServiceClient::new(tail_handle);

    // basic send in order
    for i in 1..101 {
        send_read_write(i, head_client.clone()).await;
    }

    // basic send out of order
    let mut nums: Vec<u64> = (101..201).collect();
    println!("{:?}", nums);
    nums.shuffle(&mut thread_rng());
    for i in nums.into_iter() {
        send_read_write(i, head_client.clone()).await;
    }
    
    // send a couple of reads with shared dependencies
    let read_nums: Vec<u64> = vec![1, 2, 3, 4, 5];
    for i in read_nums.into_iter() {
        send_read_only(i, Some(300), tail_client.clone()).await;
    }

    // sleep for a bit
    tokio::time::sleep(Duration::from_millis(500)).await;

    // send writes to resolve reads
    let nums: Vec<u64> = (201..301).collect();
    for i in nums.into_iter() {
        send_read_write(i, head_client.clone()).await;
    }
    // test backwards ack through chain
    let nums: Vec<u64> = (1..301).collect();
    for i in nums.into_iter() {
        tokio::spawn(send_cluster_ack(i, tail_client.clone())).await.unwrap();
    }
    
    // highly concurrent mix of reads and writes
    let mut nums: Vec<u64> = (201..501).collect();
    nums.shuffle(&mut thread_rng());
    for i in nums.into_iter() {
        tokio::spawn(send_read_write(i, head_client.clone())).await.unwrap();
        tokio::spawn(send_read_only(i, None, head_client.clone())).await.unwrap();
    }
}
