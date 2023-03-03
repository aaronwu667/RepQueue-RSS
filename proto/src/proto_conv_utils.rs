use crate::raft_net::{
    AppendEntriesRequestProto, AppendEntriesResponseProto, EntryProto, InstallSnapshotRequestProto,
    SnapshotMetaProto, VoteRequestProto, VoteResponseProto,
};
use crate::raft_net::{InstallSnapshotResponseProto, LogIdProto};
use openraft::raft::{InstallSnapshotResponse, VoteResponse};
use openraft::{
    types::v070::InstallSnapshotRequest, types::v070::VoteRequest, AppendEntriesRequest, Entry,
    SnapshotMeta,
};
use openraft::{AppData, AppendEntriesResponse, LogId};

/* Boilerplate for converting internal types to RPC requests and vice versa*/
impl<T: AppData> tonic::IntoRequest<AppendEntriesRequestProto> for AppendEntriesRequest<T> {
    fn into_request(self) -> tonic::Request<AppendEntriesRequestProto> {
        let req = AppendEntriesRequestProto {
            term: self.term,
            leader_id: self.leader_id,
            prev_log_id: self.prev_log_id.map(|lid| LogIdProto::from(lid)),
            leader_commit: self.leader_commit.map(|lid| LogIdProto::from(lid)),
            entries: self
                .entries
                .into_iter()
                .map(|ent| EntryProto::from(ent))
                .collect(),
        };
        tonic::Request::new(req)
    }
}

impl<T: AppData> From<AppendEntriesRequestProto> for AppendEntriesRequest<T> {
    fn from(r: AppendEntriesRequestProto) -> AppendEntriesRequest<T> {
        AppendEntriesRequest {
            term: r.term,
            leader_id: r.leader_id,
            prev_log_id: r.prev_log_id.map(|l| LogId::from(l)),
            entries: r.entries.into_iter().map(|ent| Entry::from(ent)).collect(),
            leader_commit: r.leader_commit.map(|l| LogId::from(l)),
        }
    }
}

impl tonic::IntoRequest<InstallSnapshotRequestProto> for InstallSnapshotRequest {
    fn into_request(self) -> tonic::Request<InstallSnapshotRequestProto> {
        let req = InstallSnapshotRequestProto {
            term: self.term,
            leader_id: self.leader_id,
            offset: self.offset,
            data: self.data,
            done: self.done,
            meta: Some(SnapshotMetaProto::from(self.meta)),
        };
        tonic::Request::new(req)
    }
}

impl From<InstallSnapshotRequestProto> for InstallSnapshotRequest {
    fn from(r: InstallSnapshotRequestProto) -> Self {
        InstallSnapshotRequest {
            term: r.term,
            leader_id: r.leader_id,
            meta: SnapshotMeta::from(r.meta.unwrap()),
            offset: r.offset,
            data: r.data,
            done: r.done,
        }
    }
}

impl tonic::IntoRequest<VoteRequestProto> for VoteRequest {
    fn into_request(self) -> tonic::Request<VoteRequestProto> {
        let req = VoteRequestProto {
            term: self.term,
            candidate_id: self.candidate_id,
            last_log_id: self.last_log_id.map(|lid| LogIdProto::from(lid)),
        };
        tonic::Request::new(req)
    }
}

impl From<VoteRequestProto> for VoteRequest {
    fn from(value: VoteRequestProto) -> Self {
        Self {
            term: value.term,
            candidate_id: value.candidate_id,
            last_log_id: value.last_log_id.map(|l| LogId::from(l)),
        }
    }
}

impl From<LogIdProto> for LogId {
    fn from(l: LogIdProto) -> LogId {
        LogId {
            term: l.term,
            index: l.index,
        }
    }
}

impl From<LogId> for LogIdProto {
    fn from(l: LogId) -> Self {
        Self {
            term: l.term,
            index: l.index,
        }
    }
}

impl<T: AppData> From<Entry<T>> for EntryProto {
    fn from(ent: Entry<T>) -> Self {
        match serde_json::to_string(&ent.payload) {
            Ok(s) => EntryProto {
                log_id: Some(LogIdProto::from(ent.log_id)),
                payload: s,
            },
            Err(_) => panic!("Payload serialization error"),
        }
    }
}

impl<T: AppData> From<EntryProto> for Entry<T> {
    fn from(ent: EntryProto) -> Self {
        match serde_json::from_str(&ent.payload) {
            Ok(p) => Entry::<T> {
                log_id: ent.log_id.map(|l| LogId::from(l)).unwrap(),
                payload: p,
            },
            Err(_) => panic!("payload deserialization erro"),
        }
    }
}

impl From<SnapshotMeta> for SnapshotMetaProto {
    fn from(meta: SnapshotMeta) -> Self {
        SnapshotMetaProto {
            last_log_id: meta.last_log_id.map(|lid| LogIdProto::from(lid)),
            snapshot_id: meta.snapshot_id,
        }
    }
}

impl From<SnapshotMetaProto> for SnapshotMeta {
    fn from(meta: SnapshotMetaProto) -> Self {
        SnapshotMeta {
            last_log_id: meta.last_log_id.map(|l| LogId::from(l)),
            snapshot_id: meta.snapshot_id,
        }
    }
}

impl From<AppendEntriesResponse> for AppendEntriesResponseProto {
    fn from(r: AppendEntriesResponse) -> Self {
        AppendEntriesResponseProto {
            term: r.term,
            success: r.success,
            conflict: r.conflict,
        }
    }
}

impl From<InstallSnapshotResponse> for InstallSnapshotResponseProto {
    fn from(r: InstallSnapshotResponse) -> Self {
        InstallSnapshotResponseProto {
            term: r.term,
            last_applied: r.last_applied.map(|l| LogIdProto::from(l)),
        }
    }
}

impl From<VoteResponse> for VoteResponseProto {
    fn from(value: VoteResponse) -> Self {
        Self {
            term: value.term,
            vote_granted: value.vote_granted,
            last_log_id: value.last_log_id.map(|l| LogIdProto::from(l)),
        }
    }
}
