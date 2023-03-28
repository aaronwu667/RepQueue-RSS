use std::collections::HashMap;

use proto::common_decls::TransactionOp;

pub mod client_session;

pub type ReadWriteTransaction = HashMap<String, TransactionOp>;
pub type ReadOnlyTransaction = Vec<String>;
