use crate::{query::scan::ScanType, record::schema::Schema};

pub trait Plan {
    fn open(&mut self, is_mutable: bool) -> ScanType;
    fn blocks_accessed(&self) -> i32;
    fn records_output(&self) -> i32;
    fn distinct_values(&self, fldname: String) -> i32;
    fn schema(&self) -> Schema;
}
