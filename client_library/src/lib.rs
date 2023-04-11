
pub use proto::common_decls::TransactionOp as TransactionOp;
pub use proto::common_decls::transaction_op::Op as Op;
pub mod client_session;
pub mod retwis_workload;

pub fn new_op(op: Op) -> TransactionOp {
    TransactionOp {
        serve_remote_groups: vec![],
        op: Some(op)
    }
}
