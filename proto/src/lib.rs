use std::collections::HashMap;

use common_decls::Csn;
use client_lib::SessionRespReadRequest;
use common_decls::{TxnRes, ValueField};

mod proto_conv_utils;
pub mod raft_net {
    tonic::include_proto!("raft_net");
}

pub mod common_decls {
    tonic::include_proto!("common_decls");
}

pub mod shard_net {
    tonic::include_proto!("shard_net");
}

pub mod client_lib {
    tonic::include_proto!("client_lib");
}

pub mod manager_net {
    tonic::include_proto!("manager_net");
}

pub mod cluster_management_net {
    tonic::include_proto!("cluster_management_net");
}

pub mod chain_management_net {
    tonic::include_proto!("chain_management_net");
}

impl ValueField {
    pub fn new(o: Option<String>) -> Self {
        ValueField { value: o }
    }
}

impl TxnRes {
    pub fn new(map: HashMap<String, ValueField>) -> Self {
        TxnRes { map }
    }
}

impl SessionRespReadRequest {
    pub fn new(res: HashMap<String, ValueField>, csn: Option<Csn>, fence: u64, num_shards: u32) -> Self {
        SessionRespReadRequest {
            res,
            csn,
            fence,
            num_shards,
        }
    }
}
