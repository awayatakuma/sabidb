use std::sync::{Arc, Mutex};

use crate::{query::scan::Scan, record::schema::Schema};

use super::{plan::Plan, product_plan::ProductPlan};

pub struct OptimizedProductPlan {
    bestplan: Arc<Mutex<dyn Plan>>,
}

impl Plan for OptimizedProductPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>, String> {
        self.bestplan
            .lock()
            .map_err(|_| "failed to get lock")?
            .open()
    }
    fn blocks_accessed(&self) -> Result<i32, String> {
        self.bestplan
            .lock()
            .map_err(|_| "failed to get lock")?
            .blocks_accessed()
    }

    fn records_output(&self) -> Result<i32, String> {
        self.bestplan
            .lock()
            .map_err(|_| "failed to get lock")?
            .records_output()
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        self.bestplan
            .lock()
            .map_err(|_| "failed to get lock")?
            .distinct_values(fldname)
    }

    fn schema(&self) -> Result<Schema, String> {
        self.bestplan
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()
    }
}

impl OptimizedProductPlan {
    pub fn _new(p1: Arc<Mutex<dyn Plan>>, p2: Arc<Mutex<dyn Plan>>) -> Result<Self, String> {
        let prod1 = ProductPlan::new(p1.clone(), p2.clone())?;
        let prod2 = ProductPlan::new(p2.clone(), p1.clone())?;
        let bestplan = if prod1.blocks_accessed() < prod2.blocks_accessed() {
            prod1
        } else {
            prod2
        };

        Ok(OptimizedProductPlan {
            bestplan: Arc::new(Mutex::new(bestplan)),
        })
    }
}
