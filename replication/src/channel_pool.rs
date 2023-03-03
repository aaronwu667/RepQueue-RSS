use openraft::NodeId;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::transport::Channel;

// A thread-safe pool of connections
pub struct ChannelPool {
    channels: RwLock<HashMap<NodeId, Channel>>,
}

impl ChannelPool {
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
        }
    }

    // block until all connections established
    // just use static conf for convenience
    // TODO (low priority): allow dynamic connection management
    #[tokio::main]
    pub async fn init(&mut self, a: Vec<String>) {
        let mut channels = self.channels.write().await;
        for (i, addr) in a.into_iter().enumerate() {
            match Channel::from_shared(addr.to_owned()) {
                Ok(chan) => match chan.connect().await {
                    Ok(connection) => {
                        channels.insert(i as NodeId, connection);
                    }
                    _ => panic!("Connection to node {} failed with addr {}", i, addr),
                },
                _ => panic!("Malformed address {}", addr),
            }
        }
    }

    // should pass in callback to return some sort of client
    pub async fn get_client<T>(&self, f: fn(Channel) -> T, node: NodeId) -> T {
        let channels = self.channels.read().await;
        match channels.get(&node) {
            Some(chan) => return f(chan.clone()),
            None => panic!("No channel associated with node {}", node),
        }
    }
}
