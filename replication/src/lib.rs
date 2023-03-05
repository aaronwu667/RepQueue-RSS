use network::BaseNetwork;
use openraft::{AppData, AppDataResponse, Raft};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use version_store::MemStore;
pub mod channel_pool;
pub mod cluster_management_service;
pub mod network;
pub mod raft_service;
pub mod shard_service;
mod utils;
pub mod version_store;

pub type RaftRepl = Raft<StoreRequest, StoreResponse, BaseNetwork, MemStore>;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Op {
    Put(String),
    Get,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StoreRequest {
    pub subtxn: HashMap<String, Op>,
    pub ind: u64,
    pub ssn: u64,
}

impl AppData for StoreRequest {}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StoreResponse {
    pub res: Option<HashMap<String, Option<String>>>,
}

impl AppDataResponse for StoreResponse {}

#[cfg(test)]
mod tests {
    use crate::version_store::MemStore;
    use crate::StoreRequest;
    use crate::StoreResponse;
    use async_trait::async_trait;
    use openraft::testing::StoreBuilder;
    use tokio::sync::watch;
    struct MemStoreBuilder {}

    #[async_trait]
    impl StoreBuilder<StoreRequest, StoreResponse, MemStore> for MemStoreBuilder {
        async fn build(&self) -> MemStore {
            let (wn, _) = watch::channel(0);
            let (rn, _) = watch::channel(0);
            MemStore::new(rn, wn)
        }
    }

    #[test]
    pub fn test_mem_store() -> anyhow::Result<()> {
        openraft::testing::Suite::test_all(MemStoreBuilder {})
    }
}
