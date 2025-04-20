use std::{
    cmp::Ordering,
    sync::{Arc, Mutex},
};

use crate::query::scan::Scan;

#[derive(Clone)]
pub struct RecordComparator {
    fields: Vec<String>,
}

impl RecordComparator {
    pub fn new(fields: Vec<String>) -> Self {
        RecordComparator { fields }
    }
    pub fn compare(
        &self,
        s1: &Arc<Mutex<dyn Scan>>,
        s2: &Arc<Mutex<dyn Scan>>,
    ) -> Result<Ordering, String> {
        for fldname in self.fields.iter() {
            let val1 = s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(fldname)?;

            let val2 = s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(fldname)?;
            if let Some(result) = val1.partial_cmp(&val2) {
                if result != Ordering::Equal {
                    return Ok(result);
                }
            }
        }
        Ok(Ordering::Equal)
    }
}
