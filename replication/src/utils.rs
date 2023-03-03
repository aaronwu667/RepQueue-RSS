use std::cmp::Ordering;
use std::collections::HashMap;

use crate::{Op::*, StoreResponse};
use proto::common_decls::transaction_op::Op::*;
use proto::shard_net::{ExecAppendRequest, ExecReadRequest};
use proto::common_decls::{ValueField, TxnRes};

use crate::StoreRequest;

// Wrapper type to enable sorting of requests
pub struct ReadRequestWrap(pub ExecReadRequest);

pub fn add_remote_deps(
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

impl PartialEq for ReadRequestWrap {
    fn eq(&self, other: &Self) -> bool {
        let own_csn = self.0.csn.as_ref().unwrap();
        let other_csn = other.0.csn.as_ref().unwrap();
        own_csn.cid == other_csn.cid && own_csn.sn == other_csn.sn
    }
}

impl Eq for ReadRequestWrap {}

// ordering based on fence, with cid breaking ties
impl PartialOrd for ReadRequestWrap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReadRequestWrap {
    fn cmp(&self, other: &Self) -> Ordering {
        let own_csn = self.0.csn.as_ref().unwrap();
        let other_csn = other.0.csn.as_ref().unwrap();
        let own_fence = self.0.fence;
        let other_fence = other.0.fence;
        if own_fence < other_fence {
            Ordering::Less
        } else if own_fence > other_fence {
            Ordering::Greater
        } else if own_csn.cid < other_csn.cid {
            Ordering::Less
        } else if own_csn.cid > other_csn.cid {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}
