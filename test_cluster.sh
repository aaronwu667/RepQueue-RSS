#!/bin/bash
cargo build

cargo run --bin repl_store -- http://127.0.0.1:5001 http://127.0.0.1:5002 http://127.0.0.1:5003 http://127.0.0.1:5004 127.0.0.1:5002 127.0.0.1:7001 1 0 > 1.log& 
cargo run --bin repl_store -- http://127.0.0.1:5001 http://127.0.0.1:5002 http://127.0.0.1:5003 http://127.0.0.1:5004 127.0.0.1:5003 127.0.0.1:7002 2 0 > 2.log&
cargo run --bin repl_store -- http://127.0.0.1:5001 http://127.0.0.1:5002 http://127.0.0.1:5003 http://127.0.0.1:5004 127.0.0.1:5004 127.0.0.1:7003 3 0 > 3.log&

cargo run --bin txn_manager -- http://127.0.0.1:5002 http://127.0.0.1:5005 127.0.0.1:5001 127.0.0.1:7004 2&
cargo run --bin txn_manager -- http://127.0.0.1:5002 http://127.0.0.1:5001 127.0.0.1:5005 127.0.0.1:7005 0&

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

# start test
cargo test repl_store_test -- --nocapture http://127.0.0.1:5002 
