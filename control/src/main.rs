use replication::{ClusterManagementServiceClient, Empty, NodeId};
use serde::Deserialize;
use std::{collections::HashMap, env, fs};
use txn_management::ChainManagementServiceClient;

#[derive(Debug, Deserialize)]
struct ServerAddrs {
    control: String,
    cluster: String,
}

impl ServerAddrs {
    fn use_uri(&mut self) {
        self.control = get_uri(&self.control);
        self.cluster = get_uri(&self.cluster);
    }
}

#[derive(Debug, Deserialize)]
struct Config {
    num_shards: usize,
    repl_factor: usize,
    raft_servers: Vec<ServerAddrs>,
    chain_servers: Vec<ServerAddrs>,
}

fn get_uri(host: &str) -> String {
    let mut uri = "http://".to_owned();
    uri.push_str(host);
    uri
}

#[tokio::main]
async fn main() {
    // basic control plane setup
    // assumed that server binaries are already running
    let mut args: Vec<String> = env::args().skip(1).collect();
    let config_path = args.pop().unwrap();
    let config_file = fs::read_to_string(config_path).expect("Invalid path or unable to open file");
    let mut config: Config = serde_json::from_str(&config_file).expect("Error parsing config file");

    assert!(
        config.repl_factor * config.num_shards == config.raft_servers.len(),
        "Wrong number of raft group addresses provided"
    );
    assert!(
        config.chain_servers.len() > 1,
        "Single chain node mode not implemented"
    );

    // turn everything into a uri
    config.raft_servers.iter_mut().for_each(|s| s.use_uri());
    config.chain_servers.iter_mut().for_each(|s| s.use_uri());

    // Form raft groups
    let mut raft_group_leaders = HashMap::new();
    let mut raft_groups = Vec::new();
    let mut curr_group = HashMap::new();
    for (i, addr) in config.raft_servers.into_iter().enumerate() {
        if i % config.repl_factor == 0 {
            let ind = u32::try_from(i / config.repl_factor).unwrap();
            raft_group_leaders.insert(ind, addr.cluster.clone());
            if i != 0 {
                raft_groups.push(curr_group);
                curr_group = HashMap::new();
            }
        }
        let cluster_ind = u64::try_from((i % config.repl_factor) + 1).unwrap();
        curr_group.insert(cluster_ind, addr);
    }
    raft_groups.push(curr_group);

    println!("{:?}", raft_groups);
    let mut chain_handles = Vec::new();

    // bring chain online
    let chain_servers = config.chain_servers;
    let tail_addr = chain_servers.last().unwrap().cluster.clone();
    for (i, addr) in chain_servers.iter().enumerate() {
        let control_uri = addr.control.clone();
        let mut client = ChainManagementServiceClient::connect(control_uri)
            .await
            .unwrap();
        let req = match i {
            0 => txn_management::InitNodeRequest {
                leader_addrs: raft_group_leaders.clone(),
                pred: None,
                succ: Some(chain_servers[i + 1].cluster.clone()),
                node_type: 0,
            },
            _ if i == chain_servers.len() - 1 => txn_management::InitNodeRequest {
                leader_addrs: raft_group_leaders.clone(),
                pred: Some(chain_servers[i - 1].cluster.clone()),
                succ: None,
                node_type: 2,
            },
            _ => txn_management::InitNodeRequest {
                leader_addrs: raft_group_leaders.clone(),
                pred: Some(chain_servers[i - 1].cluster.clone()),
                succ: Some(chain_servers[i + 1].cluster.clone()),
                node_type: 1,
            },
        };
        if let Err(e) = client.init_node(req).await {
            panic!("Initialization failed {}", e);
        }
        chain_handles.push(client)
    }

    // bring raft groups online
    let mut leader_handles = Vec::new();
    for (sid, group) in raft_groups.iter().enumerate() {
        for (nid, replica) in group.iter() {
            let control_uri = replica.control.clone();
            let mut client = ClusterManagementServiceClient::connect(control_uri)
                .await
                .unwrap();
            let mut group_addrs: HashMap<_, _> =
                group.iter().map(|(k, v)| (*k, v.cluster.clone())).collect();
            group_addrs.remove(nid);
            group_addrs.insert(0, tail_addr.clone());
            println!("{:?}", group_addrs);
            let req = replication::InitNodeRequest {
                cluster_addrs: group_addrs,
                node_id: *nid,
                shard_id: u32::try_from(sid).unwrap(),
            };
            if let Err(e) = client.init_node(req).await {
                panic!("Initialization failed {}", e)
            }
            if *nid == 1 {
                leader_handles.push(client);
            }
        }
    }

    // configure raft clusters
    for client in leader_handles.iter_mut() {
        client.init_leader(Empty {}).await.unwrap();
        for i in 1..config.repl_factor - 1 {
            let req = NodeId {
                id: u64::try_from(i + 1).unwrap(),
            };
            client.add_member(req).await.unwrap();
        }
        client.start_cluster(Empty {}).await.unwrap();
    }
}
