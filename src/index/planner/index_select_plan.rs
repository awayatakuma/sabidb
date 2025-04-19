use std::sync::{Arc, Mutex};

use crate::{
    index::query::index_select_scan::IndexSelectScan,
    metadata::index_info::IndexInfo,
    plan::plan::Plan,
    query::{
        constant::Constant,
        scan::{self, Scan},
    },
    record::schema::Schema,
};

pub struct IndexSelectPlan {
    p: Arc<Mutex<dyn Plan>>,
    ii: IndexInfo,
    val: Constant,
}

impl IndexSelectPlan {
    pub fn new(p: Arc<Mutex<dyn Plan>>, ii: IndexInfo, val: Constant) -> Self {
        IndexSelectPlan {
            p: p,
            ii: ii,
            val: val,
        }
    }
}

impl Plan for IndexSelectPlan {
    fn open(&self) -> Result<Arc<Mutex<(dyn scan::Scan + 'static)>>, String> {
        let s = self.p.lock().map_err(|_| "failed to get lock")?.open()?;
        // throws an exception if p is not a tableplan.
        let mut binding = s.lock().map_err(|_| "failed to get lock")?;
        let ts = binding.as_table_scan()?;
        let idx = self.ii.open()?;
        Ok(Arc::new(Mutex::new(IndexSelectScan::new(
            ts.clone(),
            idx,
            self.val.clone(),
        ))))
    }
    fn blocks_accessed(&self) -> Result<i32, String> {
        Ok(self.ii.blocks_accessed()? + self.records_output()?)
    }

    fn records_output(&self) -> Result<i32, String> {
        Ok(self.ii.records_output())
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        Ok(self.ii.distinct_values(fldname))
    }

    fn schema(&self) -> Result<Schema, String> {
        self.p
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()
            .clone()
    }
}
