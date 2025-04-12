use std::{cell::RefCell, rc::Rc};

use crate::query::scan::Scan;

use super::{plan::Plan, product_plan::ProductPlan};

pub struct OptimizedProductPlan {
    bestplan: Box<dyn Plan>,
}

impl Plan for OptimizedProductPlan {
    fn open(&mut self) -> Result<Box<dyn Scan>, String> {
        self.bestplan.open()
    }

    fn blocks_accessed(&self) -> Result<i32, String> {
        self.bestplan.blocks_accessed()
    }

    fn records_output(&self) -> Result<i32, String> {
        self.bestplan.records_output()
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        self.bestplan.distinct_values(fldname)
    }

    fn schema(&self) -> crate::record::schema::Schema {
        self.bestplan.schema()
    }
}

impl OptimizedProductPlan {
    pub fn new(
        p1: Rc<RefCell<Box<dyn Plan>>>,
        p2: Rc<RefCell<Box<dyn Plan>>>,
    ) -> Result<Self, String> {
        let prod1 = ProductPlan::new(p1.clone(), p2.clone())?;
        let prod2 = ProductPlan::new(p2.clone(), p1.clone())?;
        let bestplan = if prod1.blocks_accessed() < prod2.blocks_accessed() {
            prod1
        } else {
            prod2
        };

        Ok(OptimizedProductPlan {
            bestplan: Box::new(bestplan),
        })
    }
}
