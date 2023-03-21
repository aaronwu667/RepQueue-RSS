use std::collections::HashMap;

use crate::{Op::*, StoreResponse};
use proto::common_decls::transaction_op::Op::*;
use proto::common_decls::{TxnRes, ValueField};
use proto::shard_net::ExecAppendRequest;

use crate::StoreRequest;

pub(crate) fn add_remote_deps(
    k: String,
    v: Option<String>,
    map: &mut HashMap<u64, HashMap<String, ValueField>>,
    remote_groups: Vec<u64>,
) {
    for group in remote_groups {
        map.entry(group)
            .and_modify(|m| {
                m.insert(k.to_owned(), ValueField::new(v.to_owned()));
            })
            .or_insert_with(|| {
                let mut new_map = HashMap::new();
                new_map.insert(k.to_owned(), ValueField::new(v.to_owned()));
                return new_map;
            });
    }
}

impl From<&ExecAppendRequest> for StoreRequest {
    fn from(value: &ExecAppendRequest) -> Self {
        // in the future, condition parsing/eval goes here
        StoreRequest {
            subtxn: value
                .txn
                .iter()
                .map(|(k, v)| {
                    let op = match &v.op {
                        Some(Read(_)) => Get,
                        Some(Write(write_val)) => Put(write_val.to_owned()),
                        Some(CondWrite(t)) => crate::Op::Put(t.val.to_owned()),
                        None => panic!("Operation empty!"),
                    };
                    (k.to_owned(), op)
                })
                .collect(),
            ind: value.ind,
            ssn: value.sn,
        }
    }
}

impl From<StoreResponse> for Option<TxnRes> {
    fn from(value: StoreResponse) -> Self {
        value.res.map(|m| {
            let mut map = HashMap::new();
            for (k, v) in m.into_iter() {
                map.insert(k, ValueField::new(v));
            }
            TxnRes { map }
        })
    }
}
