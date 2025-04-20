use std::sync::{Arc, Mutex};

use crate::{plan::plan::Plan, record::schema::Schema, tx::transaction::Transaction};

use super::{aggregation_fn::AggregationFn, group_by_scan::GroupByScan, sort_plan::SortPlan};

pub struct GroupByPlan {
    p: Arc<Mutex<dyn Plan>>,
    groupfields: Vec<String>,
    aggfns: Vec<Arc<Mutex<dyn AggregationFn>>>,
    sch: Arc<Mutex<Schema>>,
}

impl GroupByPlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        p: Arc<Mutex<dyn Plan>>,
        groupfields: Vec<String>,
        aggfns: Vec<Arc<Mutex<dyn AggregationFn>>>,
    ) -> Result<Self, String> {
        let sortplan = SortPlan::new(tx, p, groupfields.clone())?;
        let mut sch = Schema::new();
        for fldname in groupfields.iter() {
            sch.add(fldname, Arc::new(Mutex::new(sortplan.schema()?)))?;
        }

        Ok(GroupByPlan {
            p: Arc::new(Mutex::new(sortplan)),
            groupfields: groupfields,
            aggfns: aggfns,
            sch: Arc::new(Mutex::new(sch)),
        })
    }
}

impl Plan for GroupByPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn crate::query::scan::Scan>>, String> {
        let s = self.p.lock().map_err(|_| "failed to get lock")?.open()?;
        Ok(Arc::new(Mutex::new(GroupByScan::new(
            s,
            self.groupfields.clone(),
            self.aggfns.clone(),
        )?)))
    }

    fn blocks_accessed(&self) -> Result<i32, String> {
        self.p
            .lock()
            .map_err(|_| "failed to get lock")?
            .blocks_accessed()
    }

    fn records_output(&self) -> Result<i32, String> {
        let mut numgroups = 1;
        for fldname in self.groupfields.iter() {
            numgroups += self
                .p
                .lock()
                .map_err(|_| "failed to get lock")?
                .distinct_values(fldname.clone())?;
        }

        Ok(numgroups)
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        if self
            .p
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()?
            .has_field(&fldname)?
        {
            self.p
                .lock()
                .map_err(|_| "failed to get lock")?
                .distinct_values(fldname)
        } else {
            self.records_output()
        }
    }

    fn schema(&self) -> Result<Schema, String> {
        Ok(self.sch.lock().map_err(|_| "failed to get lock")?.clone())
    }
}
