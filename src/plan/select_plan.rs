use std::sync::{Arc, Mutex};

use crate::query::{predicate::Predicate, scan::Scan, select_scan::SelectScan};

use super::plan::Plan;

pub struct SelectPlan {
    p: Arc<Mutex<dyn Plan>>,
    pred: Predicate,
}

impl Plan for SelectPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>, String> {
        let s = self.p.lock().map_err(|_| "failed to get lock")?.open()?;
        Ok(Arc::new(Mutex::new(SelectScan::new(s, self.pred.clone()))))
    }
    fn blocks_accessed(&self) -> Result<i32, String> {
        Ok(self
            .p
            .lock()
            .map_err(|_| "failed to get lock")?
            .blocks_accessed()?)
    }

    fn records_output(&self) -> Result<i32, String> {
        let ro = self
            .p
            .lock()
            .map_err(|_| "failed to get lock")?
            .records_output()?;
        Ok(ro / self.pred.reduction_factor(self.p.clone()))
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        if self.pred.equate_with_constant(&fldname).is_some() {
            Ok(1)
        } else {
            if let Some(fldname2) = self.pred.equate_with_field(&fldname) {
                Ok(i32::min(
                    self.p
                        .lock()
                        .map_err(|_| "failed to get lock")?
                        .distinct_values(fldname)?,
                    self.p
                        .lock()
                        .map_err(|_| "failed to get lock")?
                        .distinct_values(fldname2)?,
                ))
            } else {
                Ok(self
                    .p
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .distinct_values(fldname)?)
            }
        }
    }

    fn schema(&self) -> Result<crate::record::schema::Schema, String> {
        self.p.lock().map_err(|_| "failed to get lock")?.schema()
    }
}

impl SelectPlan {
    pub fn new(p: Arc<Mutex<dyn Plan>>, pred: Predicate) -> Self {
        SelectPlan { p: p, pred: pred }
    }
}
