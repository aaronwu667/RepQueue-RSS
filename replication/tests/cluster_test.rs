use proto::{
    common_decls::{transaction_op, Csn, TransactionOp},
    shard_net::{shard_service_client::ShardServiceClient, ExecAppendRequest, ExecReadRequest},
};
use rand::prelude::*;
use rand::thread_rng;
use rand::Rng;
use std::{collections::HashMap, env, time::Duration};
use tonic::transport::{Channel, Endpoint};

async fn send_read_write(ind: u64, mut client: ShardServiceClient<Channel>) {
    let mut val = "Value with index: ".to_owned();
    val.push_str(&ind.to_string());
    let mut txn = HashMap::<String, TransactionOp>::new();
    txn.insert(
        ind.to_string(),
        TransactionOp {
            serve_remote_groups: vec![],
            op: Some(transaction_op::Op::Write(val)),
        },
    );
    if ind > 1 {
        txn.insert(
            thread_rng().gen_range(1..ind).to_string(),
            TransactionOp {
                serve_remote_groups: vec![],
                op: Some(transaction_op::Op::Read("".to_owned())),
            },
        );
    }
    let req = ExecAppendRequest {
        txn,
        ind,
        sn: ind,
        local_deps: None,
    };
    if let Err(e) = client.shard_exec_append(req).await {
        panic!("Error {}", e);
    }
}

async fn send_read(fence: u64, mut client: ShardServiceClient<Channel>) {
    let req = ExecReadRequest {
        txn: vec![fence.to_string()],
        csn: Some(Csn { cid: 0, sn: 0 }),
        fence,
        num_shards: 1,
        addr: "".to_owned(),
    };
    if let Err(e) = client.shard_exec_read(req).await {
        panic!("Error {}", e);
    }
}

#[tokio::test]
async fn basic_store_test() {
    // get handle
    let mut args: Vec<String> = env::args().skip(1).collect();
    let leader_addr = args.pop().unwrap();
    let leader_handle = Endpoint::from_shared(leader_addr)
        .unwrap()
        .connect()
        .await
        .unwrap();

    let client = ShardServiceClient::new(leader_handle);

    send_read(400, client.clone()).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    for i in 1..500 {
        send_read_write(i, client.clone()).await;
    }
}

// TODO (low priority): Check dependency resolution
#[tokio::test]
async fn repl_store_test() {
    // get handle
    let mut args: Vec<String> = env::args().skip(1).collect();
    let leader_addr = args.pop().unwrap();
    let leader_handle = Endpoint::from_shared(leader_addr)
        .unwrap()
        .connect()
        .await
        .unwrap();

    let client = ShardServiceClient::new(leader_handle);
    
    // basic case, 100 in order write requests
    for i in 1..101 {
        send_read_write(i, client.clone()).await;
    }

    println!("===================== Basic test ok =====================\n");
    // Out of order write requests
    let mut nums: Vec<u64> = (101..203).collect();
    nums.shuffle(&mut thread_rng());
    for i in nums.into_iter() {
        send_read_write(i, client.clone()).await;
    }

    println!("===================== Ordering test ok =====================\n");

    // mix of (highly) concurrent requests
    let mut nums: Vec<u64> = (202..502).collect();
    nums.shuffle(&mut thread_rng());
    let mut read_fences = Vec::from(&nums[0..nums.len() / 3]);
    for num in read_fences.iter() {
        println!("{}", num);
    }
    let mut fence = read_fences.pop();
    let mut write = nums.pop();
    while fence.is_some() && write.is_some() {
        let fence_inner = fence.unwrap();
        let write_inner = write.unwrap();
        tokio::spawn(send_read_write(write_inner, client.clone()))
            .await
            .unwrap();
        tokio::spawn(send_read(fence_inner, client.clone()))
            .await
            .unwrap();
        fence = read_fences.pop();
        write = nums.pop();
    }

    while write.is_some() {
        let write_inner = write.unwrap();
        tokio::spawn(send_read_write(write_inner, client.clone()))
            .await
            .unwrap();
        write = nums.pop();
    }
}
