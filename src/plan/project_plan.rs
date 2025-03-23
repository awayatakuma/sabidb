use std::sync::{Arc, Mutex};

use crate::{query::project_scan::ProjectScan, record::schema::Schema};

use super::plan::Plan;

pub struct ProjectPlan {
    p: Box<dyn Plan>,
    schema: Schema,
}

impl Plan for ProjectPlan {
    fn open(&mut self, is_mutable: bool) -> crate::query::scan::ScanType {
        let s = if let crate::query::scan::ScanType::Scan(s) = self.p.open(is_mutable) {
            s
        } else {
            panic!("Unreachable")
        };
        crate::query::scan::ScanType::Scan(Box::new(ProjectScan::new(
            s,
            self.schema.fields().lock().unwrap().clone(),
        )))
    }

    fn blocks_accessed(&self) -> i32 {
        self.p.blocks_accessed()
    }

    fn records_output(&self) -> i32 {
        self.p.records_output()
    }

    fn distinct_values(&self, fldname: String) -> i32 {
        self.p.distinct_values(fldname)
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}

impl ProjectPlan {
    pub fn new(p: Box<dyn Plan>, fieldlist: Vec<String>) -> ProjectPlan {
        let mut schema = Schema::new();
        for fld in fieldlist {
            schema.add(&fld, Arc::new(Mutex::new(p.schema()))).unwrap();
        }

        Self {
            p: p,
            schema: schema,
        }
    }
}
