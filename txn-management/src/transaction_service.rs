use std::sync::Arc;

use async_trait::async_trait;
use proto::{
    common_decls::{Empty, ExecNotifRequest},
    manager_net::{
        manager_service_server::ManagerService, AppendTransactRequest, ExecAppendTransactRequest,
        ReadOnlyTransactRequest,
    },
};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::{ManagerNodeState, NodeStatus};
mod read_transaction;
mod write_transaction;

struct TransactionService {
    // TODO (low priority): no lock timeout checking
    // TODO (low priority): dynamic reconfig and failover
    // TODO (med priority): fair queueing for read-write transactions
    state: Arc<ManagerNodeState>,
    new_rq_ch: mpsc::Sender<AppendTransactRequest>,
    exec_notif_ch: Option<mpsc::Sender<ExecNotifRequest>>,
    exec_append_ch: Option<mpsc::Sender<ExecAppendTransactRequest>>,
}

impl TransactionService {
    fn new(state: Arc<ManagerNodeState>, node_status: NodeStatus) -> Self {
        let (new_req_tx, new_req_rx) = mpsc::channel(5000);
        let (schd_tx, schd_rx) = mpsc::channel(5000);
        let mut exec_tx = None; // RPC handler -> exec notif servicer sender
        let mut exec_append_tx = None;
        let node_status = Arc::new(node_status);
        match &*node_status {
            NodeStatus::Tail(pred, _) => {
                // spawn execNotif handler
                let (tx, exec_notif_rx) = mpsc::channel(8000);
                tokio::spawn(Self::aggregate_res(
                    state.clone(),
                    exec_notif_rx,
                    pred.clone(),
                ));
                exec_tx = Some(tx);
            }
            NodeStatus::Middle(pred, _) => {
                let (tx, exec_append_rx) = mpsc::channel(8000);
                tokio::spawn(Self::proc_exec_append(
                    state.clone(),
                    exec_append_rx,
                    Some(pred.clone()),
                ));
                exec_append_tx = Some(tx);
            }
            NodeStatus::Head(_) => {
                let (tx, exec_append_rx) = mpsc::channel(8000);
                tokio::spawn(Self::proc_exec_append(state.clone(), exec_append_rx, None));
                exec_append_tx = Some(tx);
            }
        }
        tokio::spawn(Self::scheduler(
            state.clone(),
            new_req_rx,
            schd_tx,
            node_status.clone(),
        ));
        tokio::spawn(Self::proc_append(schd_rx, state.clone(), node_status));
        Self {
            state,
            new_rq_ch: new_req_tx,
            exec_notif_ch: exec_tx,
            exec_append_ch: exec_append_tx,
        }
    }
}

#[async_trait]
impl ManagerService for TransactionService {
    async fn append_transact(
        &self,
        request: Request<AppendTransactRequest>,
    ) -> Result<Response<Empty>, Status> {
        // TODO (low priority): handle dynamic addition/removal of head (watch on node state)
        if let Err(_) = self.new_rq_ch.send(request.into_inner()).await {
            panic!("New request receiver dropped");
        }

        Ok(Response::new(Empty {}))
    }

    async fn exec_notif(
        &self,
        request: Request<ExecNotifRequest>,
    ) -> Result<Response<Empty>, Status> {
        // TODO (low priority): handle dynamic addition/removal of tail (watch on node state)
        match &self.exec_notif_ch {
            Some(c) => {
                if let Err(_) = c.send(request.into_inner()).await {
                    panic!("Exec notif receiver dropped");
                }
                Ok(Response::new(Empty {}))
            }
            None => Err(Status::new(
                tonic::Code::InvalidArgument,
                "Node is not tail",
            )),
        }
    }

    async fn exec_append_transact(
        &self,
        request: Request<ExecAppendTransactRequest>,
    ) -> Result<Response<Empty>, Status> {
        match &self.exec_append_ch {
            Some(c) => {
                if let Err(_) = c.send(request.into_inner()).await {
                    panic!("Exec append receiver dropped");
                }
                Ok(Response::new(Empty {}))
            }
            None => Err(Status::new(tonic::Code::InvalidArgument, "Node is tail")),
        }
    }

    async fn read_only_transact(
        &self,
        request: Request<ReadOnlyTransactRequest>,
    ) -> Result<Response<Empty>, Status> {
        tokio::spawn(Self::proc_read(request.into_inner(), self.state.clone()));
        Ok(Response::new(Empty {}))
    }
}
