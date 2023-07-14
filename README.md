# RepQueue-RSS

This repository contains RepQueue-RSS, a system that guarantees multi-dispatch regular sequential serializability (MD-RSS). 
MD-RSS is an augmentation of Helt et. al's regular sequential serializability 
([paper](https://dl.acm.org/doi/10.1145/3477132.3483566), [code](https://github.com/princeton-sns/spanner-rss)) that accounts for asynchronous clients.
This work was done as part of an undergraduate thesis, which can be found in `docs` as a PDF. 

## Repository organization
`client_library`: as name suggests  

`control`: cluster control and initialization

`proto`: protobuf related boilerplate and type definitions  

`replication`: raft driven key/value replication

`txn_management`: as name suggests   


## Compiling and Running
Coming soon.
