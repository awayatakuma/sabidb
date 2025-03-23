use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{query::product_scan::ProductScan, record::schema::Schema};

use super::plan::Plan;

pub struct ProductPlan {
    p1: Rc<RefCell<Box<dyn Plan>>>,
    p2: Rc<RefCell<Box<dyn Plan>>>,
    schema: Schema,
}

impl Plan for ProductPlan {
    fn open(&mut self, is_mutable: bool) -> crate::query::scan::ScanType {
        let s1 =
            if let crate::query::scan::ScanType::Scan(s) = self.p1.borrow_mut().open(is_mutable) {
                s
            } else {
                panic!("Unreachable")
            };
        let s2 =
            if let crate::query::scan::ScanType::Scan(s) = self.p2.borrow_mut().open(is_mutable) {
                s
            } else {
                panic!("Unreachable")
            };

        crate::query::scan::ScanType::Scan(Box::new(ProductScan::new(s1, s2).unwrap()))
    }

    fn blocks_accessed(&self) -> i32 {
        self.p1.borrow().blocks_accessed()
            + (self.p1.borrow().records_output() * self.p2.borrow().blocks_accessed())
    }

    fn records_output(&self) -> i32 {
        self.p1.borrow().records_output() * self.p2.borrow().records_output()
    }

    fn distinct_values(&self, fldname: String) -> i32 {
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
