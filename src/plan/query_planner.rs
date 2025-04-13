use std::sync::{Arc, Mutex};

use crate::{parse::query_data::QueryData, tx::transaction::Transaction};

use super::plan::Plan;

pub trait QueryPlanner: Sync + Send {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<Mutex<dyn Plan>>, String>;
}
