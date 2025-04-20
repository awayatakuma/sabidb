use std::sync::{Arc, Mutex};

use crate::{plan::plan::Plan, record::schema::Schema, tx::transaction::Transaction};

use super::{merge_join_scan::MergeJoinScan, sort_plan::SortPlan};

pub struct MergeJoinPlan {
    p1: Arc<Mutex<dyn Plan>>,
    p2: Arc<Mutex<dyn Plan>>,
    fldname1: String,
    fldname2: String,
    sch: Schema,
}

impl MergeJoinPlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        p1: Arc<Mutex<dyn Plan>>,
        p2: Arc<Mutex<dyn Plan>>,
        fldname1: String,
        fldname2: String,
    ) -> Result<Self, String> {
        let sortlist1 = vec![fldname1.clone()];
        let p1 = SortPlan::new(tx.clone(), p1, sortlist1)?;

        let sortlist2 = vec![fldname2.clone()];
        let p2 = SortPlan::new(tx, p2, sortlist2)?;

        let mut sch = Schema::new();
        sch.add_all(Arc::new(Mutex::new(p1.schema()?)))?;
        sch.add_all(Arc::new(Mutex::new(p2.schema()?)))?;

        Ok(MergeJoinPlan {
            p1: Arc::new(Mutex::new(p1)),
            p2: Arc::new(Mutex::new(p2)),
            fldname1,
            fldname2,
            sch: sch,
        })
    }
}

impl Plan for MergeJoinPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn crate::query::scan::Scan>>, String> {
        let s1 = self.p1.lock().map_err(|_| "failed to get lock")?.open()?;
        let s2 = self
            .p2
            .lock()
            .map_err(|_| "failed to get lock")?
            .open()?
            .lock()
            .map_err(|_| "failed to get lock")?
            .as_sort_scan()?;
        Ok(Arc::new(Mutex::new(MergeJoinScan::new(
            s1,
            s2,
            self.fldname1.clone(),
            self.fldname2.clone(),
        )?)))
    }

    fn blocks_accessed(&self) -> Result<i32, String> {
        Ok(self
            .p1
            .lock()
            .map_err(|_| "failed to get lock")?
            .blocks_accessed()?
            + self
                .p2
                .lock()
                .map_err(|_| "failed to get lock")?
                .blocks_accessed()?)
    }

    fn records_output(&self) -> Result<i32, String> {
        let maxvals = i32::max(
            self.p1
                .lock()
                .map_err(|_| "failed to get lock")?
                .distinct_values(self.fldname1.clone())?,
            self.p2
                .lock()
                .map_err(|_| "failed to get lock")?
                .distinct_values(self.fldname2.clone())?,
        );

        Ok((self
            .p1
            .lock()
            .map_err(|_| "failed to get lock")?
            .records_output()?
            * self
                .p2
                .lock()
                .map_err(|_| "failed to get lock")?
                .records_output()?)
            / maxvals)
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        if self
            .p1
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()?
            .has_field(&fldname)?
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
