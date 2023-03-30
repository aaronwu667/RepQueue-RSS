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
    pub fn new(my_node_id: Option<u64>, addrs: Vec<String>) -> Self {
        let mut new_map = HashMap::new();
        let my_node_id = my_node_id.map(|num| usize::try_from(num).unwrap());
        for (i, addr) in addrs.into_iter().enumerate() {
            if let Some(nid) = my_node_id {
                if nid == i {
                    continue;
                }
            }
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
        for (i, addr) in channels.iter_mut() {
            if let ChanStatus::NotInit(endpt) = addr {
                match endpt.connect().await {
                    Ok(connection) => {
                        println!("Connection succeeded node {} with uri {}", i, endpt.uri());
                        *addr = ChanStatus::Init(connection);
                    }
                    Err(e) => {
                        return Err(format!(
                            "Connection to node {} failed with addr {}, {}",
                            i,
                            endpt.uri(),
                            e
                        ))
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn get_client<V>(&self, f: fn(Channel) -> V, node: T) -> V {
        let channels = self.channels.read().await;
        let ent = channels
            .get(&node)
            .unwrap_or_else(|| panic!("No channel associated with node {}", node));
        match ent {
            ChanStatus::Init(chan) => f(chan.clone()),
            ChanStatus::NotInit(_) => {
                panic!("Dynamic connection not implemented")
            }
        }
    }

    pub async fn get_all_nodes(&self) -> BTreeSet<T> {
        let channels = self.channels.read().await;
        channels.keys().copied().collect()
    }
}
