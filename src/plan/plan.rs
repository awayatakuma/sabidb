use crate::{query::scan::Scan, record::schema::Schema};

pub trait Plan {
    fn open(&mut self) -> Result<Box<dyn Scan>, String>;
    fn blocks_accessed(&self) -> Result<i32, String>;
    fn records_output(&self) -> Result<i32, String>;
    fn distinct_values(&self, fldname: String) -> Result<i32, String>;
    fn schema(&self) -> Schema;
}
