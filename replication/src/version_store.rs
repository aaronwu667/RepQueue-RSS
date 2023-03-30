use async_trait::async_trait;
use openraft::types::v070::Entry;
use openraft::{
    AnyError, EffectiveMembership, EntryPayload::*, ErrorSubject, ErrorVerb, HardState, LogId,
    LogState, RaftStorage, RaftStorageDebug, Snapshot, SnapshotMeta, StateMachineChanges,
    StorageError, StorageIOError,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{hash_map, BTreeMap, HashMap},
    fmt::Debug,
    io::Cursor,
    ops::Bound::*,
    ops::RangeBounds,
};
use tokio::sync::{watch, Mutex, RwLock};

use crate::Op::*;
use crate::{StoreRequest, StoreResponse};

// Just use JSON to serialize things locally
pub struct MemStoreSnapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

// application specific state
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct VersionStoreState {
    pub data: HashMap<String, BTreeMap<u64, String>>, // key |-> {(version, value)}
    last_applied: Option<LogId>,
    last_membership: Option<EffectiveMembership>,
    pub ssn: u64,
    pub sh_exec: u64,
}

impl VersionStoreState {
    pub fn put(&mut self, k: String, sn: u64, v: String) {
        let ent = self.data.entry(k);
        match ent {
            hash_map::Entry::Occupied(_) => {
                ent.and_modify(|m| {
                    m.insert(sn, v);
                });
            }
            hash_map::Entry::Vacant(_) => {
                let mut new_map = BTreeMap::new();
                new_map.insert(sn, v);
                ent.or_insert(new_map);
            }
        }
    }

    // get value of key with greatest sequence number <= sn
    pub fn get(&self, k: &String, sn: &u64) -> Option<String> {
        self.data
            .get(k)
            .and_then(|m| m.range((Unbounded, Included(sn))).next_back())
            .map(|tup| tup.1.to_owned())
    }
}

// log related state
pub struct VersionStoreLog {
    log: BTreeMap<u64, Entry<StoreRequest>>,
    last_purged: Option<LogId>,
}

// In-memory storage engine
pub struct MemStore {
    pub state: RwLock<VersionStoreState>,
    write_notif: watch::Sender<u64>,
    read_notif: watch::Sender<u64>,
    logger: RwLock<VersionStoreLog>,
    hard_state: Mutex<Option<HardState>>,
    current_snapshot: RwLock<Option<MemStoreSnapshot>>,
}

impl MemStore {
    pub fn new(wn: watch::Sender<u64>, rn: watch::Sender<u64>) -> Self {
        let state = VersionStoreState {
            data: HashMap::new(),
            last_applied: None,
            last_membership: None,
            ssn: 0,
            sh_exec: 0,
        };
        let logger = VersionStoreLog {
            log: BTreeMap::new(),
            last_purged: None,
        };
        MemStore {
            state: RwLock::new(state),
            write_notif: wn,
            read_notif: rn,
            logger: RwLock::new(logger),
            current_snapshot: RwLock::new(None),
            hard_state: Mutex::new(None),
        }
    }
}

#[async_trait]
impl RaftStorageDebug<VersionStoreState> for MemStore {
    async fn get_state_machine(&self) -> VersionStoreState {
        self.state.write().await.clone()
    }
}

#[async_trait]
impl RaftStorage<StoreRequest, StoreResponse> for MemStore {
    type SnapshotData = Cursor<Vec<u8>>;

    // --- Vote and term

    async fn save_hard_state(&self, hs: &HardState) -> Result<(), StorageError> {
        let mut hard_state = self.hard_state.lock().await;
        *hard_state = Some(hs.clone());
        Ok(())
    }

    async fn read_hard_state(&self) -> Result<Option<HardState>, StorageError> {
        let hard_state = self.hard_state.lock().await;
        Ok((*hard_state).clone())
    }

    // --- Log

    async fn get_log_state(&self) -> Result<LogState, StorageError> {
        let logger = self.logger.read().await;
        let log = &logger.log;

        let last_purged_log_id = logger.last_purged;
        let last = log.iter().rev().next().map(|(_, ent)| ent.log_id);

        let last = match last {
            None => last_purged_log_id,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id: last,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &self,
        range: RB,
    ) -> Result<Vec<Entry<StoreRequest>>, StorageError> {
        let res = {
            let log = &self.logger.read().await.log;
            log.range(range.clone())
                .map(|(_, val)| val.clone())
                .collect::<Vec<_>>()
        };

        Ok(res)
    }

    async fn append_to_log(&self, entries: &[&Entry<StoreRequest>]) -> Result<(), StorageError> {
        let mut logger = self.logger.write().await;
        for entry in entries {
            logger.log.insert(entry.log_id.index, (*entry).clone());
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(&self, log_id: LogId) -> Result<(), StorageError> {
        let log = &mut self.logger.write().await.log;

        let keys = log
            .range(log_id.index..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            log.remove(&key);
        }

        Ok(())
    }

    async fn purge_logs_upto(&self, log_id: LogId) -> Result<(), StorageError> {
        let mut logger = self.logger.write().await;

        let ld = &mut logger.last_purged;
        assert!(*ld <= Some(log_id));
        *ld = Some(log_id);

        let log = &mut logger.log;

        let keys = log
            .range(..=log_id.index)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            log.remove(&key);
        }

        Ok(())
    }

    // --- State Machine

    async fn last_applied_state(
        &self,
    ) -> Result<(Option<LogId>, Option<EffectiveMembership>), StorageError> {
        let state = self.state.read().await;
        // we can move options (there's an auto deref here) :o
        Ok((state.last_applied, state.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &self,
        entries: &[&Entry<StoreRequest>],
    ) -> Result<Vec<StoreResponse>, StorageError> {
        let mut res = Vec::with_capacity(entries.len());
        let mut state = self.state.write().await;
        for ent in entries {
            state.last_applied = Some(ent.log_id);
            match &ent.payload {
                Blank => {
                    res.push(StoreResponse { res: None });
                }
                Normal(req) => {
                    if req.ssn > state.ssn {
                        let ind = req.ind;
                        assert!(req.ssn == state.ssn + 1, "Ordering violation!");
                        assert!(state.sh_exec < ind, "Monotonicity violated!");
                        let mut ent_res: Option<HashMap<String, Option<String>>> = None;

                        // Serve reads and writes
                        for (k, op) in req.subtxn.iter() {
                            match op {
                                Put(v) => state.put(k.to_owned(), ind, v.to_owned()),
                                Get => {
                                    if let Some(ref mut m) = ent_res {
                                        m.insert(k.to_owned(), state.get(k, &ind));
                                    } else {
                                        let mut new_map = HashMap::new();
                                        new_map.insert(k.to_owned(), state.get(k, &ind));
                                        ent_res = Some(new_map);
                                    }
                                }
                            }
                        }
                        // update sequence numbers and notify
                        // Optimization: batch these notifications
                        state.ssn += 1;
                        if self.write_notif.send(state.ssn).is_err() {
                            panic!("SSN update receivers dropped");
                        }

                        state.sh_exec = req.ind;
                        if self.read_notif.send(state.sh_exec).is_err() {
                            panic!("sh_exec update receivers dropped");
                        }

                        // push result into vector
                        res.push(StoreResponse { res: ent_res });
                    }
                }
                Membership(mem) => {
                    state.last_membership = Some(EffectiveMembership {
                        log_id: ent.log_id,
                        membership: mem.clone(),
                    });
                    res.push(StoreResponse { res: None });
                }
            }
        }
        return Ok(res);
    }

    // --- Snapshot

    async fn build_snapshot(&self) -> Result<Snapshot<Self::SnapshotData>, StorageError> {
        let (data, last_applied_log);

        {
            // Serialize the data of the state machine.
            let state = self.state.read().await;
            data = serde_json::to_vec(&*state).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;

            last_applied_log = state.last_applied;
        }

        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}", last.term, last.index)
        } else {
            "No data".to_string()
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            snapshot_id,
        };

        let snapshot = MemStoreSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }

    async fn begin_receiving_snapshot(&self) -> Result<Box<Self::SnapshotData>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &self,
        meta: &SnapshotMeta,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges, StorageError> {
        let new_snapshot = MemStoreSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        let new_sm: VersionStoreState =
            serde_json::from_slice(&new_snapshot.data).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::Snapshot(new_snapshot.meta.clone()),
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;
        let mut state = self.state.write().await;
        *state = new_sm;

        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    async fn get_current_snapshot(
        &self,
    ) -> Result<Option<Snapshot<Self::SnapshotData>>, StorageError> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}
