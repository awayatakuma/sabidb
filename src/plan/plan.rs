use crate::{query::scan::Scan, record::schema::Schema};

pub trait Plan {
    fn open(&mut self) -> Box<dyn Scan>;
    fn blocks_accessed(&self) -> i32;
    fn records_output(&self) -> i32;
    fn distinct_values(&self, fldname: String) -> i32;
    fn schema(&self) -> Schema;
}
