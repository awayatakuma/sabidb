use std::sync::{Arc, Mutex};

use crate::{
    query::{product_scan::ProductScan, scan::Scan},
    record::schema::Schema,
};

use super::plan::Plan;

pub struct ProductPlan {
    p1: Arc<Mutex<dyn Plan>>,
    p2: Arc<Mutex<dyn Plan>>,
    schema: Schema,
}

impl Plan for ProductPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>, String> {
        let s1 = self.p1.lock().map_err(|_| "failed to get lock")?.open()?;
        let s2 = self.p2.lock().map_err(|_| "failed to get lock")?.open()?;
        Ok(Arc::new(Mutex::new(ProductScan::new(s1, s2)?)))
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
        let p2_ba = self
            .p2
            .lock()
            .map_err(|_| "failed to get lock")?
            .blocks_accessed()?;
        Ok(p1_ba + (p1_ro * p2_ba))
    }

    fn records_output(&self) -> Result<i32, String> {
        let p1_ro = self
            .p1
            .lock()
            .map_err(|_| "failed to get lock")?
            .records_output()?;
        let p2_ro = self
            .p2
            .lock()
            .map_err(|_| "failed to get lock")?
            .records_output()?;
        Ok(p1_ro * p2_ro)
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        if self.schema()?.has_field(&fldname).unwrap() {
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
        Ok(self.schema.clone())
    }
}

impl ProductPlan {
    pub fn new(p1: Arc<Mutex<dyn Plan>>, p2: Arc<Mutex<dyn Plan>>) -> Result<Self, String> {
        let mut sch = Schema::new();
        sch.add_all(Arc::new(Mutex::new(
            p1.lock().map_err(|_| "failed to get lock")?.schema()?,
        )))?;
        sch.add_all(Arc::new(Mutex::new(
            p2.lock().map_err(|_| "failed to get lock")?.schema()?,
        )))?;
        Ok(ProductPlan {
            p1: p1,
            p2: p2,
            schema: sch,
        })
    }
}
