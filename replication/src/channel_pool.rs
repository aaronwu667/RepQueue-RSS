use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::transport::Channel;

// A thread-safe pool of connections
// TODO (low priority): clean this mess up and dynamic connection management
pub struct ChannelPool<T> {
    channels: RwLock<HashMap<T, Channel>>,
}

impl<T> ChannelPool<T>
where
    T: std::cmp::Eq
        + std::hash::Hash
        + std::fmt::Display
        + std::convert::TryFrom<usize>
        + std::fmt::Debug,
{
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
        }
    }

    // block until all connections established
    // just use static conf for convenience
    #[tokio::main]
    pub async fn init(&mut self, a: Vec<String>)
    where
        T::Error: std::fmt::Debug,
    {
        let mut channels = self.channels.write().await;
        for (i, addr) in a.into_iter().enumerate() {
            match Channel::from_shared(addr.to_owned()) {
                Ok(chan) => match chan.connect().await {
                    Ok(connection) => {
                        channels.insert(T::try_from(i).unwrap(), connection);
                    }
                    _ => panic!("Connection to node {} failed with addr {}", i, addr),
                },
                _ => panic!("Malformed address {}", addr),
            }
        }
    }

    // should pass in callback to return some sort of client
    pub async fn get_client<V>(&self, f: fn(Channel) -> V, node: T) -> V {
        let channels = self.channels.read().await;
        match channels.get(&node) {
            Some(chan) => return f(chan.clone()),
            None => panic!("No channel associated with node {}", node),
        }
    }
}
