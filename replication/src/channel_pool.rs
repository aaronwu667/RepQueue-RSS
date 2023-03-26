use std::collections::{BTreeSet, HashMap};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

// A thread-safe pool of connections
// TODO (low priority): clean this mess up and dynamic connection management
enum ChanStatus {
    Init(Channel),
    NotInit(Endpoint),
}

pub struct ChannelPool<T> {
    channels: RwLock<HashMap<T, ChanStatus>>,
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
    pub fn new(addrs: Vec<String>) -> Self {
        let mut new_map = HashMap::new();
        for (i, addr) in addrs.into_iter().enumerate() {
            new_map.insert(
                T::try_from(i).unwrap(),
                ChanStatus::NotInit(Endpoint::from_shared(addr).unwrap()),
            );
        }
        Self {
            channels: RwLock::new(new_map),
        }
    }

    // just use static conf for convenience
    pub async fn connect(&self) -> Result<(), String> {
        let mut channels = self.channels.write().await;
        for (i, addr) in channels.values_mut().enumerate() {
            match addr {
                ChanStatus::NotInit(endpt) => match endpt.connect().await {
                    Ok(connection) => {
                        *addr = ChanStatus::Init(connection);
                    }
                    _ => {
                        return Err(format!(
                            "Connection to node {} failed with addr {}",
                            i,
                            endpt.uri()
                        ))
                    }
                },
                _ => (),
            }
        }
        Ok(())
    }

    pub async fn get_client<V>(&self, f: fn(Channel) -> V, node: T) -> V {
        let channels = self.channels.read().await;
        let ent = match channels.get(&node) {
            Some(status) => status,
            None => panic!("No channel associated with node {}", node),
        };
        match ent {
            ChanStatus::Init(chan) => return f(chan.clone()),
            ChanStatus::NotInit(_) => {
                panic!("Dynamic connection not implemented")
            }
        }
    }

    pub async fn get_all_nodes(&self) -> BTreeSet<T> {
        let channels = self.channels.read().await;
        channels.keys().map(|e| *e).collect()
    }
}
