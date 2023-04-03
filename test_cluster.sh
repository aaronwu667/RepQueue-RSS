#!/bin/bash
cargo build --release

cargo run --release --bin repl_store -- http://127.0.0.1:5001 http://127.0.0.1:5002 http://127.0.0.1:5003 http://127.0.0.1:5004 127.0.0.1:5002 127.0.0.1:7001 1 0 > test_output/1.log& 
cargo run --release --bin repl_store -- http://127.0.0.1:5001 http://127.0.0.1:5002 http://127.0.0.1:5003 http://127.0.0.1:5004 127.0.0.1:5003 127.0.0.1:7002 2 0 > test_output/2.log&
cargo run --release --bin repl_store -- http://127.0.0.1:5001 http://127.0.0.1:5002 http://127.0.0.1:5003 http://127.0.0.1:5004 127.0.0.1:5004 127.0.0.1:7003 3 0 > test_output/3.log&

cargo run --release --bin txn_manager -- http://127.0.0.1:5002 http://127.0.0.1:5005 127.0.0.1:5001 127.0.0.1:7004 2 > test_output/tail.log&
cargo run --release --bin txn_manager -- http://127.0.0.1:5002 http://127.0.0.1:5001 127.0.0.1:5005 127.0.0.1:7005 0 > test_output/head.log&

# let servers come up
sleep 5

# Connect cluster nodes first
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{}' '127.0.0.1:7001' cluster_management_net.ClusterManagementService/ConnectNode
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{}' '127.0.0.1:7002' cluster_management_net.ClusterManagementService/ConnectNode
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{}' '127.0.0.1:7003' cluster_management_net.ClusterManagementService/ConnectNode

# Connect chain nodes
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{}' '127.0.0.1:7004' cluster_management_net.ClusterManagementService/ConnectNode
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{}' '127.0.0.1:7005' cluster_management_net.ClusterManagementService/ConnectNode

sleep 1

# Init leader node
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{}' '127.0.0.1:7001' cluster_management_net.ClusterManagementService/InitLeader

sleep 0.5

# Add learners
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{"id": 2}' '127.0.0.1:7001' cluster_management_net.ClusterManagementService/AddMember
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{"id": 3}' '127.0.0.1:7001' cluster_management_net.ClusterManagementService/AddMember

sleep 0.5

# Initalize cluster
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{}' '127.0.0.1:7001' cluster_management_net.ClusterManagementService/StartCluster

sleep 0.5

# test raft group
#cargo test repl_store_test -- --nocapture http://127.0.0.1:5002

#sleep 0.5

# test chain
#cargo test chain_test -- --nocapture http://127.0.0.1:5005 http://127.0.0.1:5001

#cargo test basic_tail_test -- --nocapture http://127.0.0.1:5005 http://127.0.0.1:5001

#cargo test client_integ_test -- --nocapture http://127.0.0.1:5001 http://127.0.0.1:5005 127.0.0.1:8001 > test_output/client.log

cargo run --release --bin single_client_latency -- http://127.0.0.1:5001 http://127.0.0.1:5005 127.0.0.1:8001 100000 0.7

sleep 5

# shutdown servers after we are done
for pid in $(ps -ef | grep "repl_store" | awk '{print $2}'); do kill -9 $pid; done;
for pid in $(ps -ef | grep "txn_manager" | awk '{print $2}'); do kill -9 $pid; done;
