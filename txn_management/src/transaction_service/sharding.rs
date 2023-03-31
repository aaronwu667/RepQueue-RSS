use std::collections::BTreeMap;

#[derive(Debug)]
pub(super) struct BucketEntry {
    pub sid: u32,
    pub visited: bool,
}

impl BucketEntry {
    fn new(sid: u32) -> Self {
        Self {
            sid,
            visited: false,
        }
    }
}

pub(super) fn get_buckets(num_shards: u32) -> BTreeMap<u64, BucketEntry> {
    let step = u64::MAX / u64::from(num_shards);
    let mut rem = u64::MAX % u64::from(num_shards);
    let mut prev = 0;
    let mut buckets = BTreeMap::<u64, BucketEntry>::new();
    for i in 0..num_shards {
        prev += step;
        if rem > 0 {
            prev += 1;
            rem -= 1;
            buckets.insert(prev, BucketEntry::new(i));
        } else {
            buckets.insert(prev, BucketEntry::new(i));
        }
    }
    buckets
}
