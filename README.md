# RepQueue-RSS

This repository contains RepQueue-RSS, a system that guarantees multi-dispatch regular sequential serializability (MD-RSS). 
MD-RSS is an augmentation of Helt et. al's regular sequential serializability 
([paper](https://dl.acm.org/doi/10.1145/3477132.3483566), [code](https://github.com/princeton-sns/spanner-rss)) that accounts for asynchronous clients.
This work was done as part of an undergraduate thesis, which can be found under `docs` as a PDF. 

A modified version of Spanner-RSS served as a baseline in performance experiments. Spanner-RSS is a variant of Google's Spanner that relaxes consistency from strict serializability to regular sequential serializability. 
The baseline builds on top of Spanner-RSS by adding several ordering guarantees for asynchronous client requests. The code lives in the [spanner-rss-md](https://github.com/aaronwu667/spanner-rss-md) repository.

## Repository organization
`client_library`: as the name suggests  

`control`: cluster control and initialization

`proto`: protobuf related boilerplate and type definitions  

`replication`: Raft-driven key/value replication

`txn_management`: as the name suggests   


## Compiling and Running
Coming soon.
