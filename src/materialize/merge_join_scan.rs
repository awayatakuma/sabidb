use std::{
    cmp::Ordering,
    sync::{Arc, Mutex},
};

use crate::query::{constant::Constant, scan::Scan};

use super::sort_scan::SortScan;

pub struct MergeJoinScan {
    s1: Arc<Mutex<dyn Scan>>,
    s2: Arc<Mutex<SortScan>>,
    fldname1: String,
    fldname2: String,
    joinval: Option<Constant>,
}

impl MergeJoinScan {
    pub fn new(
        s1: Arc<Mutex<dyn Scan>>,
        s2: Arc<Mutex<SortScan>>,
        fldname1: String,
        fldname2: String,
    ) -> Result<Self, String> {
        let mut res = MergeJoinScan {
            s1,
            s2,
            fldname1,
            fldname2,
            joinval: None,
        };
        res.before_first()?;

        Ok(res)
    }
}

impl Scan for MergeJoinScan {
    fn before_first(&mut self) -> Result<(), String> {
        self.s1
            .lock()
            .map_err(|_| "failed to get lock")?
            .before_first()?;
        self.s2
            .lock()
            .map_err(|_| "failed to get lock")?
            .before_first()?;

        Ok(())
    }

    fn next(&mut self) -> Result<bool, String> {
        let mut hasmore2 = self.s2.lock().map_err(|_| "failed to get lock")?.next()?;
        if hasmore2
            && self
                .s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(&self.fldname2)?
                .eq(self.joinval.as_ref().unwrap())
        {
            return Ok(true);
        }

        let mut hasmore1 = self.s1.lock().map_err(|_| "failed to get lock")?.next()?;
        if hasmore1
            && self
                .s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(&self.fldname2)?
                .eq(self.joinval.as_ref().unwrap())
        {
            self.s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .restore_position()?;
            return Ok(true);
        }

        while hasmore1 && hasmore2 {
            let v1 = self
                .s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(&self.fldname1)?;
            let v2 = self
                .s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(&self.fldname1)?;

            match v1.partial_cmp(&v2).unwrap() {
                Ordering::Less => {
                    hasmore1 = self.s1.lock().map_err(|_| "failed to get lock")?.next()?
                }
                Ordering::Equal => {
                    self.s2
                        .lock()
                        .map_err(|_| "failed to get lock")?
                        .save_position()?;
                    self.joinval = Some(
                        self.s2
                            .lock()
                            .map_err(|_| "failed to get lock")?
                            .get_val(&self.fldname2)?,
                    );
                    return Ok(true);
                }
                Ordering::Greater => {
                    hasmore2 = self.s2.lock().map_err(|_| "failed to get lock")?.next()?
                }
            }
        }

        Ok(false)
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        if self
            .s1
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?
        {
            return self
                .s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_int(fldname);
        } else {
            return self
                .s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_int(fldname);
        }
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        if self
            .s1
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?
        {
            return self
                .s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_string(fldname);
        } else {
            return self
                .s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_string(fldname);
        }
    }

    fn get_val(&self, fldname: &String) -> Result<Constant, String> {
        if self
            .s1
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?
        {
            return self
                .s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(fldname);
        } else {
            return self
                .s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(fldname);
        }
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        Ok(self
            .s1
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?
            || self
                .s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .has_field(fldname)?)
    }

    fn close(&mut self) -> Result<(), String> {
        self.s1.lock().map_err(|_| "failed to get lock")?.close()?;
        self.s2.lock().map_err(|_| "failed to get lock")?.close()?;

        Ok(())
    }

    fn to_update_scan(
        &mut self,
    ) -> Result<Arc<Mutex<dyn crate::query::update_scan::UpdateScan>>, String> {
        todo!()
    }

    fn as_table_scan(&mut self) -> Result<&mut crate::record::table_scan::TableScan, String> {
        todo!()
    }

    fn as_sort_scan(&mut self) -> Result<Arc<Mutex<SortScan>>, String> {
        todo!()
    }
}
