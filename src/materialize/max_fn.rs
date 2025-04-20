use std::cmp::Ordering;

use crate::query::constant::Constant;

use super::aggregation_fn::AggregationFn;

pub struct MaxFn {
    fldname: String,
    val: Option<Constant>,
}

impl MaxFn {
    pub fn new(fldname: String) -> Self {
        MaxFn {
            fldname: fldname,
            val: None,
        }
    }
}

impl AggregationFn for MaxFn {
    fn process_first(
        &mut self,
        s: std::sync::Arc<std::sync::Mutex<dyn crate::query::scan::Scan>>,
    ) -> Result<(), String> {
        self.val = Some(
            s.lock()
                .map_err(|_| "failed to get lock")?
                .get_val(&self.fldname)?,
        );
        Ok(())
    }

    fn process_next(
        &mut self,
        s: std::sync::Arc<std::sync::Mutex<dyn crate::query::scan::Scan>>,
    ) -> Result<(), String> {
        let newval = Some(
            s.lock()
                .map_err(|_| "failed to get lock")?
                .get_val(&self.fldname)?,
        );
        if newval.partial_cmp(&self.val) == Some(Ordering::Greater) {
            self.val = newval;
        }
        Ok(())
    }

    fn field_name(&self) -> Result<String, String> {
        Ok(format!("maxof {}", self.fldname))
    }

    fn value(&self) -> Result<Constant, String> {
        Ok(self.val.clone().unwrap())
    }
}
