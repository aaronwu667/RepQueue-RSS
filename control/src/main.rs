use replication::{ClusterManagementServiceClient, Empty};
use serde::Deserialize;
use std::{env, fs};
use txn_management::ChainManagementServiceClient;

#[derive(Debug, Deserialize)]
struct Config {
    cluster_port: u16, // data plane port
    control_port: u16, // control plane port
    num_shards: usize,
    repl_factor: usize,
    raft_servers: Vec<String>,
    chain_servers: Vec<String>,
}

fn get_uri(host: &str, port: u16) -> String {
    let mut uri = "http://".to_owned();
    uri.push_str(host);
    uri.push_str(&port.to_string());
    uri
}

#[tokio::main]
async fn main() {
    // basic control plane setup
    // assumed that server binaries are already running
    let mut args: Vec<String> = env::args().skip(1).collect();
    let config_path = args.pop().unwrap();
    let config_file = fs::read_to_string(config_path).expect("Invalid path or unable to open file");
    let config: Config = serde_json::from_str(&config_file).expect("Error parsing config file");

    assert!(
        config.repl_factor * config.num_shards == config.raft_servers.len(),
        "Wrong number of raft group addresses provided"
    );
    assert!(
        config.chain_servers.len() > 1,
        "Single chain node mode not implemented"
    );

    // Form raft groups
    let mut raft_group_leaders = Vec::new();
    let mut raft_groups = Vec::new();
    let mut curr_group = Vec::new();
    for (i, addr) in config.raft_servers.into_iter().enumerate() {
        if i % config.repl_factor == 0 {
            raft_group_leaders.push(addr.clone());
            if i != 0 {
                raft_groups.push(curr_group);
                curr_group = Vec::new();
            }
        }
        curr_group.push(addr);
    }

    let mut chain_handles = Vec::new();

    // bring chain online
    let chain_servers = config.chain_servers;
    let tail_addr = get_uri(chain_servers.last().unwrap(), config.cluster_port);
    for (i, addr) in chain_servers.iter().enumerate() {
        let control_uri = get_uri(addr, config.control_port);
        let mut client = ChainManagementServiceClient::connect(control_uri)
            .await
            .unwrap();
        let req = match i {
            0 => txn_management::InitNodeRequest {
                leader_addrs: raft_group_leaders.clone(),
                pred: None,
                succ: Some(get_uri(&chain_servers[i + 1], config.cluster_port)),
                node_type: 0,
            },
            _ if i == chain_servers.len() - 1 => txn_management::InitNodeRequest {
                leader_addrs: raft_group_leaders.clone(),
                pred: Some(get_uri(&chain_servers[i - 1], config.cluster_port)),
                succ: None,
                node_type: 2,
            },
            _ => txn_management::InitNodeRequest {
                leader_addrs: raft_group_leaders.clone(),
                pred: Some(get_uri(&chain_servers[i - 1], config.cluster_port)),
                succ: Some(get_uri(&chain_servers[i + 1], config.cluster_port)),
                node_type: 1,
            },
        };
        if let Err(e) = client.init_node(req).await {
            panic!("Initialization failed {}", e);
        }
        chain_handles.push(client)
    }

    // bring raft groups online
    let mut cluster_handles = Vec::new();
    for (sid, group) in raft_groups.iter().enumerate() {
        for (nid, replica) in group.iter().enumerate() {
            let control_uri = get_uri(replica, config.control_port);
            let mut client = ClusterManagementServiceClient::connect(control_uri)
                .await
                .unwrap();
            let mut group_addrs = vec![tail_addr.clone()];
            group_addrs.extend(
                group
                    .iter()
                    .map(|e| get_uri(e, config.cluster_port))
                    .collect::<Vec<String>>(),
            );
            let req = replication::InitNodeRequest {
                cluster_addrs: group_addrs,
                node_id: u64::try_from(nid + 1).unwrap(),
                shard_id: u32::try_from(sid).unwrap(),
            };
            if let Err(e) = client.init_node(req).await {
                panic!("Initialization failed {}", e)
            }
            cluster_handles.push(client);
        }
    }

    // connect everything
    for client in cluster_handles.iter_mut() {
        if let Err(e) = client.connect_node(Empty {}).await {
            panic!("Connection failed {}", e);
        }
    }
    for client in chain_handles.iter_mut() {
        if let Err(e) = client.connect_node(Empty {}).await {
            panic!("Connection failed {}", e);
        }
    }

    // configure raft clusters
    
}
