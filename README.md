# RepQueue-RSS

This repository contains RepQueue-RSS, a system that guarantees multi-dispatch regular sequential serializability (MD-RSS). 
MD-RSS is an augmentation of Helt et. al's regular sequential serializability 
([paper](https://dl.acm.org/doi/10.1145/3477132.3483566), [code](https://github.com/princeton-sns/spanner-rss)) that accounts for asynchronous clients.
This work was done as part of an undergraduate thesis, which can be found under `docs` as a PDF. 

A modified version of Spanner-RSS served as a baseline in performance experiments. Spanner-RSS is a variant of Google's Spanner that relaxes consistency from strict serializability to regular sequential serializability. 
The baseline builds on top of Spanner-RSS by adding several ordering guarantees for asynchronous client requests. Its code can be found in the [spanner-rss-md](https://github.com/aaronwu667/spanner-rss-md) repository.

## Repository organization
`client_library`: as the name suggests

`control`: cluster control and initialization

`proto`: protobuf related boilerplate and type definitions

`replication`: Raft-driven key/value replication

`txn_management`: as the name suggests


## Compiling and Running
To compile, simply run `cargo build` in the base directory with desired compilation flags. Ensure that the build finishes successfully before continuing.

`run_cluster.py` performs the latency experiments described in the thesis. Various settings can be adjusted through `config.json`. At the minimum, you should change `experiment_path` to the base directory's absolute path in your environment. After experiment completion, use `kill_cluster.py` for clean up. 

If you wish to use RepQueue-RSS outside of the provided experiment, initializing a working system consists of:
1. Bringing storage and transaction management nodes online.
2. Creating a .json configuration file containing the node addresses.
3. Running the cluster control binary, with path to the above config file as a command line argument.

See `get_init_cluster` in `run_cluster.py` for a concrete example. 

Once initialized, your clients should interact with the system through instances of `ClientSession`, defined in the `client_library` crate. Contained within the same crate, `latency.rs` demonstrates API usage.
