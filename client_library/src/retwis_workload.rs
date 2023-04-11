use crate::client_session::ClientSession;
use crate::{new_op, Op};
use proto::common_decls::TransactionOp;
use rand::prelude::Distribution;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::collections::HashMap;

pub enum RetwisTransactionType {
    Follow,
    PostTweet,
    GetTimeline,
}

pub enum RetwisTransaction {
    RO(Vec<String>),
    RW(HashMap<String, TransactionOp>),
}

impl RetwisTransaction {
    pub async fn eval(self, cs: &ClientSession) -> (u64, Option<HashMap<String, Option<String>>>) {
        match self {
            Self::RO(vec) => cs.read_only_transaction(vec).await,
            Self::RW(map) => cs.read_write_transaction(map).await,
        }
    }
}

pub struct RetwisTransactionBuilder {}

impl RetwisTransactionBuilder {    
    pub fn get_next_transaction(txn_type: RetwisTransactionType, num_keys: usize, skew: f64) -> RetwisTransaction {
        let zipf = zipf::ZipfDistribution::new(num_keys, skew).unwrap();
        let rng = rand::thread_rng();
        match txn_type {
            RetwisTransactionType::Follow => follow(zipf, rng),
            RetwisTransactionType::PostTweet => post_tweet(zipf, rng),
            RetwisTransactionType::GetTimeline => get_timeline(zipf, rng),
        }
    }
}

fn follow(distr: zipf::ZipfDistribution, mut rng: ThreadRng) -> RetwisTransaction {
    let mut rw = HashMap::new();
    let get_key = distr.sample(&mut rng);
    rw.insert(get_key.to_string(), new_op(Op::Read("".to_owned())));
    rw.insert(get_key.to_string(), new_op(Op::Write(get_key.to_string())));
    let get_key = distr.sample(&mut rng);
    rw.insert(get_key.to_string(), new_op(Op::Read("".to_owned())));
    rw.insert(get_key.to_string(), new_op(Op::Write(get_key.to_string())));
    RetwisTransaction::RW(rw)
}

fn post_tweet(distr: zipf::ZipfDistribution, mut rng: ThreadRng) -> RetwisTransaction {
    let mut rw = HashMap::new();
    for _ in 0..3 {
        let key = distr.sample(&mut rng);
        rw.insert(key.to_string(), new_op(Op::Read("".to_owned())));
        rw.insert(key.to_string(), new_op(Op::Write(key.to_string())));
    }
    for _ in 0..2 {
        let put_key = distr.sample(&mut rng);
        rw.insert(put_key.to_string(), new_op(Op::Write(put_key.to_string())));
    }
    RetwisTransaction::RW(rw)
}

fn get_timeline(distr: zipf::ZipfDistribution, mut rng: ThreadRng) -> RetwisTransaction {
    let num_keys_read = rng.gen_range(1..=10);
    let mut reads = Vec::with_capacity(num_keys_read);
    for _ in 0..num_keys_read {
        reads.push(distr.sample(&mut rng).to_string());
    }
    RetwisTransaction::RO(reads)
}
