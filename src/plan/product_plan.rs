use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{
    query::{product_scan::ProductScan, scan::Scan},
    record::schema::Schema,
};

use super::plan::Plan;

pub struct ProductPlan {
    p1: Rc<RefCell<Box<dyn Plan>>>,
    p2: Rc<RefCell<Box<dyn Plan>>>,
    schema: Schema,
}

impl Plan for ProductPlan {
    fn open(&mut self) -> Result<Box<dyn Scan>, String> {
        let s1 = self.p1.borrow_mut().open()?;
        let s2 = self.p2.borrow_mut().open()?;
        Ok(Box::new(ProductScan::new(s1, s2)?))
    }

    fn blocks_accessed(&self) -> Result<i32, String> {
        Ok(self.p1.borrow().blocks_accessed()?
            + (self.p1.borrow().records_output()? * self.p2.borrow().blocks_accessed()?))
    }

    fn records_output(&self) -> Result<i32, String> {
        Ok(self.p1.borrow().records_output()? * self.p2.borrow().records_output()?)
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        if self.schema().has_field(&fldname).unwrap() {
            self.p1.borrow().distinct_values(fldname)
        } else {
            self.p2.borrow().distinct_values(fldname)
        }
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}

impl ProductPlan {
    pub fn new(
        p1: Rc<RefCell<Box<dyn Plan>>>,
        p2: Rc<RefCell<Box<dyn Plan>>>,
    ) -> Result<Self, String> {
        let mut sch = Schema::new();
        sch.add_all(Arc::new(Mutex::new(p1.borrow().schema())))?;
        sch.add_all(Arc::new(Mutex::new(p2.borrow().schema())))?;
        Ok(ProductPlan {
            p1: p1,
            p2: p2,
            schema: sch,
        })
    }
}
