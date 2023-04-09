use futures::FutureExt;
use proto::manager_net::manager_service_client::ManagerServiceClient;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::transport::{Channel, Endpoint};

// reexport
pub use proto::chain_management_net::chain_management_service_client::ChainManagementServiceClient;
pub use proto::chain_management_net::InitNodeRequest;

pub mod chain_management_service;
pub mod transaction_service;

const DEBUG: bool = false;

fn debug(a: String) {
    if DEBUG {
        println!("{}", a);
    }
}

#[allow(dead_code)]
fn is_sorted<T>(data: &[T]) -> bool
where
    T: Ord,
{
    data.windows(2).all(|w| w[0] <= w[1])
}

pub struct Connection {
    endpoint: Endpoint,
    chan: OnceCell<Channel>,
}

// TODO: change to watch channel
impl Connection {
    fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            chan: OnceCell::new(),
        }
    }
    async fn get_client(&self) -> ManagerServiceClient<Channel> {
        let chan = self
            .chan
            .get_or_init(|| self.endpoint.connect().map(|r| r.unwrap()))
            .await;
        ManagerServiceClient::new(chan.clone())
    }
}

// TODO (low priority): Either add a single node variant or refactor this
pub enum NodeStatus {
    Head(Arc<Connection>),
    Tail(Arc<Connection>),
    Middle(Arc<Connection>, Arc<Connection>),
}

impl NodeStatus {
    pub fn new_head(e: Endpoint) -> Self {
        Self::Head(Arc::new(Connection::new(e)))
    }
    pub fn new_tail(e: Endpoint) -> Self {
        Self::Tail(Arc::new(Connection::new(e)))
    }
    pub fn new_middle(pred: Endpoint, succ: Endpoint) -> Self {
        Self::Middle(
            Arc::new(Connection::new(pred)),
            Arc::new(Connection::new(succ)),
        )
    }
}
