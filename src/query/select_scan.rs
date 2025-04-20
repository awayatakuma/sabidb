use std::sync::{Arc, Mutex};

use crate::materialize::sort_scan::SortScan;

use super::{predicate::Predicate, scan::Scan, update_scan::UpdateScan};

#[derive(Clone)]
pub struct SelectScan {
    s: Arc<Mutex<dyn Scan>>,
    pred: Predicate,
}

impl SelectScan {
    pub fn new(s: Arc<Mutex<dyn Scan>>, pred: Predicate) -> Self {
        SelectScan { s: s, pred }
    }
}

impl Scan for SelectScan {
    fn before_first(&mut self) -> Result<(), String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .before_first()
    }

    fn next(&mut self) -> Result<bool, String> {
        while self.s.lock().map_err(|_| "failed to get lock")?.next()? {
            if self.pred.is_satisfied(self.s.clone())? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_int(fldname)
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_string(fldname)
    }

    fn get_val(&self, fldname: &String) -> Result<super::constant::Constant, String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_val(fldname)
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)
    }

    fn close(&mut self) -> Result<(), String> {
        self.s.lock().map_err(|_| "failed to get lock")?.close()
    }

    fn to_update_scan(&mut self) -> Result<Arc<Mutex<(dyn UpdateScan + 'static)>>, String> {
        Ok(Arc::new(Mutex::new(self.clone())))
    }

    fn as_table_scan(&mut self) -> Result<&mut crate::record::table_scan::TableScan, String> {
        Err("Unexpected downcast".to_string())
    }

    fn as_sort_scan(&mut self) -> Result<Arc<Mutex<SortScan>>, String> {
        Err("Unexpected downcast".to_string())
    }
}

impl UpdateScan for SelectScan {
    fn set_val(&mut self, fldname: String, val: super::constant::Constant) -> Result<(), String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .to_update_scan()?
            .lock()
            .map_err(|_| "failed to get lock")?
            .set_val(fldname, val)
    }

    fn set_int(&mut self, fldname: String, val: i32) -> Result<(), String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .to_update_scan()?
            .lock()
            .map_err(|_| "failed to get lock")?
            .set_int(fldname, val)
    }

    fn set_string(&mut self, fldname: String, val: String) -> Result<(), String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .to_update_scan()?
            .lock()
            .map_err(|_| "failed to get lock")?
            .set_string(fldname, val)
    }

    fn insert(&mut self) -> Result<(), String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .to_update_scan()?
            .lock()
            .map_err(|_| "failed to get lock")?
            .insert()
    }

    fn delete(&mut self) -> Result<(), String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .to_update_scan()?
            .lock()
            .map_err(|_| "failed to get lock")?
            .delete()
    }

    fn get_rid(&mut self) -> Result<crate::record::rid::RID, String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .to_update_scan()?
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_rid()
    }

    fn move_to_rid(&mut self, rid: crate::record::rid::RID) -> Result<(), String> {
        self.s
            .lock()
            .map_err(|_| "failed to get lock")?
            .to_update_scan()?
            .lock()
            .map_err(|_| "failed to get lock")?
            .move_to_rid(rid)
    }
    fn to_scan(&mut self) -> Result<Arc<Mutex<dyn Scan>>, String> {
        Ok(Arc::new(Mutex::new(self.clone())))
    }
}
