use std::sync::{Arc, Mutex};

use crate::query::{constant::Constant, scan::Scan};
pub trait AggregationFn {
    fn process_first(&mut self, s: Arc<Mutex<dyn Scan>>) -> Result<(), String>;
    fn process_next(&mut self, s: Arc<Mutex<dyn Scan>>) -> Result<(), String>;
    fn field_name(&self) -> Result<String, String>;
    fn value(&self) -> Result<Constant, String>;
}
