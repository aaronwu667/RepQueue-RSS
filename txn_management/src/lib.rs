use proto::manager_net::manager_service_client::ManagerServiceClient;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

pub mod cluster_management_service;
pub mod transaction_service;

const DEBUG : bool = false;

fn debug(a: String) {
    if DEBUG {
        println!("{}", a);
    }
}

fn is_sorted<T>(data: &[T]) -> bool
where
    T: Ord,
{
    data.windows(2).all(|w| w[0] <= w[1])
}

enum ConnectionStatus {
    Init(Channel),
    NotInit(Endpoint),
}

pub struct Connection {
    inner: RwLock<ConnectionStatus>,
}

// TODO: change to watch channel
impl Connection {
    fn new(e: Endpoint) -> Self {
        Self {
            inner: RwLock::new(ConnectionStatus::NotInit(e)),
        }
    }
    async fn get_client(&self) -> ManagerServiceClient<Channel> {
        let status = self.inner.read().await;
        match &*status {
            ConnectionStatus::Init(ch) => ManagerServiceClient::new(ch.clone()),
            _ => panic!("Dynamic connection not implemented"),
        }
    }
    async fn connect(&self) -> Result<(), String> {
        let mut status = self.inner.write().await;
        match &*status {
            ConnectionStatus::NotInit(endpt) => match endpt.connect().await {
                Ok(ch) => {
                    println!("Connection succeeded {}", endpt.uri());
                    *status = ConnectionStatus::Init(ch);
                    Ok(())
                }
                Err(_) => Err(format!("conenction failed with addr {}", endpt.uri())),
            },
            _ => Ok(()),
        }
    }
}

// TODO (low priority): Either add a single variant or refactor this
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
    pub async fn connect(&self) -> Result<(), String> {
        match self {
            Self::Head(c) => Ok(c.connect().await?),
            Self::Tail(c) => Ok(c.connect().await?),
            Self::Middle(c, d) => {
                c.connect().await?;
                d.connect().await?;
                Ok(())
            }
        }
    }
}
