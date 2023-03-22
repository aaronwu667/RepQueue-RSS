use std::sync::Arc;

use proto::manager_net::ReadOnlyTransactRequest;

use crate::ManagerNodeState;

use super::TransactionService;

// Read only transactions
impl TransactionService {
    pub(super) async fn proc_read(request: ReadOnlyTransactRequest, state: Arc<ManagerNodeState>) {
        
    }
}
