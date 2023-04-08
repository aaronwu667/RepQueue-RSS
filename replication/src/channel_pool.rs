use futures::FutureExt;
use std::{
    collections::hash_map::Entry::*,
    collections::{BTreeSet, HashMap},
    sync::Arc,
};
use tokio::sync::{OnceCell, RwLock};
use tonic::transport::{Channel, Endpoint};

// A thread-safe pool of connections
// TODO (low priority): clean this mess up and dynamic connection management
struct Connection {
    endpoint: Endpoint,
    chan: OnceCell<Channel>,
}

impl Connection {
    fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            chan: OnceCell::new(),
        }
    }
    async fn get_client<V>(&self, fun: fn(Channel) -> V) -> V {
        let chan = self
            .chan
            .get_or_init(|| self.endpoint.connect().map(|r| r.unwrap()))
            .await;
        fun(chan.clone())
    }
}

#[derive(Default)]
pub struct ChannelPool<T> {
    // TODO Change to oncecell
    channels: RwLock<HashMap<T, Arc<Connection>>>,
}

impl<T> ChannelPool<T>
where
    T: std::cmp::Eq
        + std::hash::Hash
        + std::fmt::Display
        + std::convert::TryFrom<usize>
        + std::fmt::Debug
        + Copy
        + std::cmp::Ord,
    T::Error: std::fmt::Debug,
{
    pub fn new() -> Self {
        let new_map = HashMap::new();
        Self {
            channels: RwLock::new(new_map),
        }
    }

    pub async fn add_addrs_lazy(&self, addrs: HashMap<T, String>) {
        let mut map = self.channels.write().await;
        for (i, addr) in addrs.into_iter() {
            let endpoint = Endpoint::from_shared(addr).unwrap();
            map.insert(i, Arc::new(Connection::new(endpoint)));
        }
    }

    pub async fn add_addr_eager<V>(&self, node: T, addr: String, fun: fn(Channel) -> V) -> V {
        let mut channels = self.channels.write().await;
        let ent = channels.entry(node);
        let chan_status = match ent {
            Occupied(o) => o.get().clone(),
            Vacant(v) => {
                let endpoint = Endpoint::from_shared(addr).unwrap();
                let new_status = Arc::new(Connection::new(endpoint));
                v.insert(new_status.clone());
                new_status
            }
        };
        drop(channels);

        chan_status.get_client(fun).await
    }

    pub async fn get_client<V>(&self, node: T, fun: fn(Channel) -> V) -> Option<V> {
        let channels = self.channels.read().await;
        let ent = channels.get(&node);
        let chan_status = match ent {
            Some(status) => status.clone(),
            None => return None,
        };
        drop(channels);

        Some(chan_status.get_client(fun).await)
    }

    pub async fn get_all_nodes(&self) -> BTreeSet<T> {
        let channels = self.channels.read().await;
        channels.keys().copied().collect()
    }
}
