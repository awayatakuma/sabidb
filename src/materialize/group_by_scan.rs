use std::sync::{Arc, Mutex};

use crate::query::scan::Scan;

use super::{aggregation_fn::AggregationFn, group_value::GroupValue};

pub struct GroupByScan {
    s: Arc<Mutex<dyn Scan>>,
    groupfields: Vec<String>,
    aggfns: Vec<Arc<Mutex<dyn AggregationFn>>>,
    groupval: Option<GroupValue>,
    moregroups: bool,
}

impl GroupByScan {
    pub fn new(
        s: Arc<Mutex<dyn Scan>>,
        groupfields: Vec<String>,
        aggfns: Vec<Arc<Mutex<dyn AggregationFn>>>,
    ) -> Result<Self, String> {
        let mut ret = GroupByScan {
            s,
            groupfields,
            aggfns,
            groupval: None,
            moregroups: false,
        };
        ret.before_first()?;

        Ok(ret)
    }
}

impl Scan for GroupByScan {
    fn before_first(&mut self) -> Result<(), String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .before_first()?;
        self.moregroups = self.s.lock().map_err(|_| "failed to get lock")?.next()?;

        Ok(())
    }

    fn next(&mut self) -> Result<bool, String> {
        if !self.moregroups {
            return Ok(false);
        }
        for func in self.aggfns.iter_mut() {
            func.lock()
                .map_err(|_| "failed to get lock")?
                .process_first(self.s.clone())?;
        }
        self.groupval = Some(GroupValue::new(self.s.clone(), self.groupfields.clone())?);
        while self.s.lock().map_err(|_| "failed to get lock")?.next()? {
            let gv = GroupValue::new(self.s.clone(), self.groupfields.clone())?;
            if let Some(groupval) = self.groupval.as_ref() {
                if groupval.eq(&gv) {
                    break;
                }
            }
            for func in self.aggfns.iter_mut() {
                func.lock()
                    .map_err(|_| "failed to get lock")?
                    .process_next(self.s.clone())?;
            }
        }

        Ok(true)
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        self.get_val(fldname)?
            .as_int()
            .ok_or("invalid type".to_string())
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        self.get_val(fldname)?
            .as_string()
            .ok_or("invalid type".to_string())
    }

    fn get_val(&self, fldname: &String) -> Result<crate::query::constant::Constant, String> {
        if self.groupfields.contains(fldname) {
            return Ok(self.groupval.as_ref().unwrap().get_val(fldname).unwrap());
        }
        for func in self.aggfns.iter() {
            if func
                .lock()
                .map_err(|_| "failed to get lock")?
                .field_name()?
                .eq(fldname)
            {
                return func.lock().map_err(|_| "failed to get lock")?.value();
            }
        }
        Err(format!("field {} not found", fldname))
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        if self.groupfields.contains(fldname) {
            return Ok(true);
        }
        for func in &self.aggfns {
            if func
                .lock()
                .map_err(|_| "failed to get lock")?
                .field_name()?
                .eq(fldname)
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn close(&mut self) -> Result<(), String> {
        self.s.lock().map_err(|_| "failed to get lock")?.close()
    }

    fn to_update_scan(
        &mut self,
    ) -> Result<Arc<Mutex<dyn crate::query::update_scan::UpdateScan>>, String> {
        Err("Unexpected downcast".to_string())
    }

    fn as_table_scan(&mut self) -> Result<&mut crate::record::table_scan::TableScan, String> {
        Err("Unexpected downcast".to_string())
    }

    fn as_sort_scan(&mut self) -> Result<Arc<Mutex<super::sort_scan::SortScan>>, String> {
        Err("Unexpected downcast".to_string())
    }
}
