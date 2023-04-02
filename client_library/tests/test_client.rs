use client_library::{client_session::ClientSession, Op, TransactionOp};
use proto::client_lib::client_library_server::ClientLibraryServer;
use std::{collections::HashMap, env};
use tonic::transport::Server;

#[tokio::test]
async fn client_integ_test() {
    let mut args: Vec<String> = env::args().skip(1).collect();
    let my_addr = args.pop().unwrap();
    let head_addr = args.pop().unwrap();
    let chain_node = args.pop().unwrap();
    let mut my_serv_addr = "http://".to_owned();
    my_serv_addr.push_str(&my_addr);
    let (client_lib, server) = ClientSession::new(my_serv_addr, head_addr, chain_node).await;

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

    // Basic transaction
    let mut basic_req = HashMap::<String, TransactionOp>::new();
    basic_req.insert(
        1.to_string(),
        TransactionOp {
            serve_remote_groups: vec![],
            op: Some(Op::Write("Hello".to_owned())),
        },
    );
    basic_req.insert(
        3.to_string(),
        TransactionOp {
            serve_remote_groups: vec![],
            op: Some(Op::Write("Hello".to_owned())),
        },
    );

    client_lib.read_write_transaction(basic_req).await;
    let res = client_lib
        .read_only_transaction(vec![1.to_string(), 3.to_string()])
        .await;
    println!("{:?}", res);
}
