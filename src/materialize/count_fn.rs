use crate::query::constant::Constant;

use super::aggregation_fn::AggregationFn;

struct CountFn {
    fldname: String,
    count: i32,
}

impl CountFn {
    pub fn new(fldname: String) -> Self {
        CountFn {
            fldname: fldname,
            count: 0,
        }
    }
}

impl AggregationFn for CountFn {
    fn process_first(
        &mut self,
        _s: std::sync::Arc<std::sync::Mutex<dyn crate::query::scan::Scan>>,
    ) -> Result<(), String> {
        self.count = 1;
        Ok(())
    }

    fn process_next(
        &mut self,
        _s: std::sync::Arc<std::sync::Mutex<dyn crate::query::scan::Scan>>,
    ) -> Result<(), String> {
        self.count += 1;
        Ok(())
    }

    fn field_name(&self) -> Result<String, String> {
        Ok(format!("countof {}", self.fldname))
    }

    fn value(&self) -> Result<crate::query::constant::Constant, String> {
        Ok(Constant::new_from_i32(self.count))
    }
}
