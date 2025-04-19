use std::sync::{Arc, Mutex};

use crate::{
    index::query::index_join_scan::IndexJoinScan, metadata::index_info::IndexInfo,
    plan::plan::Plan, query::scan, record::schema::Schema,
};

pub struct IndexJoinPlan {
    p1: Arc<Mutex<dyn Plan>>,
    p2: Arc<Mutex<dyn Plan>>,
    ii: IndexInfo,
    joinfield: String,
    sch: Schema,
}

impl IndexJoinPlan {
    pub fn new(
        p1: Arc<Mutex<dyn Plan>>,
        p2: Arc<Mutex<dyn Plan>>,
        ii: IndexInfo,
        joinfield: String,
    ) -> Result<Self, String> {
        let mut sch = Schema::new();
        sch.add_all(Arc::new(Mutex::new(
            p1.lock().map_err(|_| "failed to get lock")?.schema()?,
        )))?;
        sch.add_all(Arc::new(Mutex::new(
            p2.lock().map_err(|_| "failed to get lock")?.schema()?,
        )))?;
        Ok(IndexJoinPlan {
            p1: p1,
            p2: p2,
            ii: ii,
            joinfield: joinfield,
            sch: sch,
        })
    }
}

impl Plan for IndexJoinPlan {
    fn open(&self) -> Result<std::sync::Arc<std::sync::Mutex<(dyn scan::Scan + 'static)>>, String> {
        let s1 = self.p1.lock().map_err(|_| "failed to get lock")?.open()?;
        let s2 = self.p2.lock().map_err(|_| "failed to get lock")?.open()?;
        // throws an exception if p is not a tableplan.
        let mut binding = s2.lock().map_err(|_| "failed to get lock")?;
        let ts = binding.as_table_scan()?;
        let idx = self.ii.open()?;
        Ok(Arc::new(Mutex::new(IndexJoinScan::new(
            s1,
            idx,
            self.joinfield.clone(),
            Arc::new(Mutex::new(ts.clone())),
        ))))
    }

    fn blocks_accessed(&self) -> Result<i32, String> {
        let p1_ba = self
            .p1
            .lock()
            .map_err(|_| "failed to get lock")?
            .blocks_accessed()?;
        let p1_ro = self
            .p1
            .lock()
            .map_err(|_| "failed to get lock")?
            .records_output()?;
        Ok(p1_ba + p1_ro * self.ii.blocks_accessed()? + self.records_output()?)
    }

    fn records_output(&self) -> Result<i32, String> {
        Ok(self
            .p1
            .lock()
            .map_err(|_| "failed to get lock")?
            .records_output()?
            * self.ii.records_output())
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        if self
            .p1
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()?
            .has_field(&fldname)
            .unwrap()
        {
            self.p1
                .lock()
                .map_err(|_| "failed to get lock")?
                .distinct_values(fldname)
        } else {
            self.p2
                .lock()
                .map_err(|_| "failed to get lock")?
                .distinct_values(fldname)
        }
    }

    fn schema(&self) -> Result<Schema, String> {
        Ok(self.sch.clone())
    }
}
