use std::sync::{Arc, Mutex};

use crate::{query::scan::Scan, record::schema::Schema};

pub trait Plan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>, String>;
    fn blocks_accessed(&self) -> Result<i32, String>;
    fn records_output(&self) -> Result<i32, String>;
    fn distinct_values(&self, fldname: String) -> Result<i32, String>;
    fn schema(&self) -> Result<Schema, String>;
}
