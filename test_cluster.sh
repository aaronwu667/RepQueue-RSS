#!/bin/bash

cargo run --bin repl_store -- http://127.0.0.1:5001 http://127.0.0.1:5002 http://127.0.0.1:5003 http://127.0.0.1:5004 127.0.0.1:5002 127.0.0.1:7001 1 0&
cargo run --bin repl_store -- http://127.0.0.1:5001 http://127.0.0.1:5002 http://127.0.0.1:5003 http://127.0.0.1:5004 127.0.0.1:5003 127.0.0.1:7002 2 0&
cargo run --bin repl_store -- http://127.0.0.1:5001 http://127.0.0.1:5002 http://127.0.0.1:5003 http://127.0.0.1:5004 127.0.0.1:5004 127.0.0.1:7003 3 0&

cargo run --bin txn_manager -- http://127.0.0.1:5002 http://127.0.0.1:5005 127.0.0.1:5001 127.0.0.1:7004 2&
cargo run --bin txn_manager -- http://127.0.0.1:5002 http://127.0.0.1:5001 127.0.0.1:5005 127.0.0.1:7005 0&

# let servers come up
sleep 10

# Connect cluster nodes first
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{}' '127.0.0.1:7001' cluster_management_net.ClusterManagementService/ConnectNode
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{}' '127.0.0.1:7002' cluster_management_net.ClusterManagementService/ConnectNode
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{}' '127.0.0.1:7003' cluster_management_net.ClusterManagementService/ConnectNode

# Connect chain nodes
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{}' '127.0.0.1:7004' cluster_management_net.ClusterManagementService/ConnectNode
grpcurl -plaintext -import-path ./proto/src/protodefs/ -proto clustermanagement.proto -d '{}' '127.0.0.1:7005' cluster_management_net.ClusterManagementService/ConnectNode

sleep 1

# Init leader node




