use std::sync::{Arc, Mutex};

use crate::{
    query::{project_scan::ProjectScan, scan::Scan},
    record::schema::Schema,
};

use super::plan::Plan;

pub struct ProjectPlan {
    p: Arc<Mutex<dyn Plan>>,
    schema: Schema,
}

impl Plan for ProjectPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>, String> {
        let s = self.p.lock().map_err(|_| "failed to get lock")?.open()?;
        Ok(Arc::new(Mutex::new(ProjectScan::new(
            s,
            self.schema
                .fields()
                .lock()
                .map_err(|_| "failed to get lock")?
                .clone(),
        ))))
    }

    fn blocks_accessed(&self) -> Result<i32, String> {
        self.p
            .lock()
            .map_err(|_| "failed to get lock")?
            .blocks_accessed()
    }

    fn records_output(&self) -> Result<i32, String> {
        self.p
            .lock()
            .map_err(|_| "failed to get lock")?
            .records_output()
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        self.p
            .lock()
            .map_err(|_| "failed to get lock")?
            .distinct_values(fldname)
    }

    fn schema(&self) -> Result<Schema, String> {
        Ok(self.schema.clone())
    }
}

impl ProjectPlan {
    pub fn new(p: Arc<Mutex<dyn Plan>>, fieldlist: Vec<String>) -> Result<ProjectPlan, String> {
        let mut schema = Schema::new();
        for fld in fieldlist {
            schema
                .add(
                    &fld,
                    Arc::new(Mutex::new(
                        p.lock().map_err(|_| "failed to get lock")?.schema()?,
                    )),
                )
                .unwrap();
        }

        Ok(Self { p: p, schema })
    }
}
