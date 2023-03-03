use network::BaseNetwork;
use openraft::{AppData, AppDataResponse, Raft};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use version_store::MemStore;
mod channel_pool;
mod network;
mod version_store;
mod raft_service;
mod shard_service;
mod utils;

pub type RaftRepl = Raft<StoreRequest, StoreResponse, BaseNetwork, Arc<MemStore>>;

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
    use crate::Arc;
    use async_trait::async_trait;
    use openraft::testing::StoreBuilder;
    use tokio::sync::Notify;
    struct MemStoreBuilder {}

    #[async_trait]
    impl StoreBuilder<StoreRequest, StoreResponse, Arc<MemStore>> for MemStoreBuilder {
        async fn build(&self) -> Arc<MemStore> {
	    let wn = Arc::new(Notify::new());
	    let rn = Arc::new(Notify::new());
            Arc::new(MemStore::new(&rn, &wn))
        }
    }

    #[test]
    pub fn test_mem_store() -> anyhow::Result<()> {
        openraft::testing::Suite::test_all(MemStoreBuilder {})
    }
}
