use std::sync::{Arc, Mutex};

use crate::{
    query::{project_scan::ProjectScan, scan::Scan},
    record::schema::Schema,
};

use super::plan::Plan;

pub struct ProjectPlan {
    p: Box<dyn Plan>,
    schema: Schema,
}

impl Plan for ProjectPlan {
    fn open(&mut self) -> Result<Box<dyn Scan>, String> {
        let s = self.p.open()?;
        Ok(Box::new(ProjectScan::new(
            s,
            self.schema
                .fields()
                .lock()
                .map_err(|_| "failed to get lock")?
                .clone(),
        )))
    }

    fn blocks_accessed(&self) -> Result<i32, String> {
        self.p.blocks_accessed()
    }

    fn records_output(&self) -> Result<i32, String> {
        self.p.records_output()
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
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
