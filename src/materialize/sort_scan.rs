use std::{
    cmp::Ordering,
    sync::{Arc, Mutex},
};

use crate::{
    query::{scan::Scan, update_scan::UpdateScan},
    record::rid::RID,
};

use super::{record_comparator::RecordComparator, temp_table::TempTable};

#[derive(Clone)]
enum CurrentScan {
    S1,
    S2,
}

#[derive(Clone)]
pub struct SortScan {
    s1: Arc<Mutex<dyn UpdateScan>>,
    s2: Option<Arc<Mutex<dyn UpdateScan>>>,
    currentscan: Option<CurrentScan>,
    comp: RecordComparator,
    hasmore1: bool,
    hasmore2: bool,
    savedpoint: Vec<RID>,
}

impl SortScan {
    pub fn new(runs: Vec<TempTable>, comp: RecordComparator) -> Result<Self, String> {
        let s1 = runs.get(0).unwrap().open()?;
        let hasmore1 = s1.lock().map_err(|_| "failed to get lock")?.next()?;
        let mut s2 = None;
        let mut hasmore2 = false;

        if let Some(temp_file) = runs.get(1) {
            let s = temp_file.open()?;
            hasmore2 = s.lock().map_err(|_| "failed to get lock")?.next()?;
            s2 = Some(s);
        }

        Ok(SortScan {
            s1: s1,
            s2: s2,
            currentscan: None,
            comp: comp,
            hasmore1: hasmore1,
            hasmore2: hasmore2,
            savedpoint: Vec::new(),
        })
    }

    pub fn save_position(&mut self) -> Result<(), String> {
        let mut lst = Vec::new();

        lst.push(
            self.s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_rid()?,
        );
        if let Some(s2) = self.s2.as_ref() {
            lst.push(s2.lock().map_err(|_| "failed to get lock")?.get_rid()?);
        }

        self.savedpoint = lst;

        Ok(())
    }

    pub fn restore_position(&self) -> Result<(), String> {
        if let Some(rid1) = self.savedpoint.get(0) {
            self.s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .move_to_rid(rid1.clone())?;
        }
        if let Some(rid2) = self.savedpoint.get(0) {
            self.s2
                .as_ref()
                .unwrap()
                .lock()
                .map_err(|_| "failed to get lock")?
                .move_to_rid(rid2.clone())?;
        }
        Ok(())
    }
}

impl Scan for SortScan {
    fn before_first(&mut self) -> Result<(), String> {
        self.currentscan = None;
        let mut s1 = self.s1.lock().map_err(|_| "failed to get lock")?;
        s1.before_first()?;
        self.hasmore1 = s1.next()?;
        if let Some(s2) = &self.s2 {
            s2.lock()
                .map_err(|_| "failed to get lock")?
                .before_first()?;
            self.hasmore2 = s2.lock().map_err(|_| "failed to get lock")?.next()?;
        }

        Ok(())
    }

    fn next(&mut self) -> Result<bool, String> {
        if let Some(currentscan) = &self.currentscan {
            match currentscan {
                CurrentScan::S1 => {
                    self.hasmore1 = self.s1.lock().map_err(|_| "failed to get lock")?.next()?
                }
                CurrentScan::S2 => {
                    self.hasmore2 = self
                        .s2
                        .as_ref()
                        .unwrap()
                        .lock()
                        .map_err(|_| "failed to get lock")?
                        .next()?
                }
            }
        }
        if !self.hasmore1 && !self.hasmore2 {
            return Ok(false);
        } else if self.hasmore1 && self.hasmore2 {
            if self.comp.compare(
                &self
                    .s1
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .to_scan()?,
                &self
                    .s2
                    .as_ref()
                    .unwrap()
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .to_scan()?,
            )? == Ordering::Less
            {
                self.currentscan = Some(CurrentScan::S1);
            } else {
                self.currentscan = Some(CurrentScan::S2);
            }
            return Ok(false);
        } else if self.hasmore1 {
            self.currentscan = Some(CurrentScan::S1);
        } else if self.hasmore2 {
            self.currentscan = Some(CurrentScan::S2);
        }

        Ok(true)
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        match self.currentscan.as_ref().unwrap() {
            CurrentScan::S1 => self
                .s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_int(fldname),
            CurrentScan::S2 => self
                .s2
                .as_ref()
                .unwrap()
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_int(fldname),
        }
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        match self.currentscan.as_ref().unwrap() {
            CurrentScan::S1 => self
                .s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_string(fldname),
            CurrentScan::S2 => self
                .s2
                .as_ref()
                .unwrap()
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_string(fldname),
        }
    }

    fn get_val(&self, fldname: &String) -> Result<crate::query::constant::Constant, String> {
        match self.currentscan.as_ref().unwrap() {
            CurrentScan::S1 => self
                .s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(fldname),
            CurrentScan::S2 => self
                .s2
                .as_ref()
                .unwrap()
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(fldname),
        }
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        match self.currentscan.as_ref().unwrap() {
            CurrentScan::S1 => self
                .s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .has_field(fldname),
            CurrentScan::S2 => self
                .s2
                .as_ref()
                .unwrap()
                .lock()
                .map_err(|_| "failed to get lock")?
                .has_field(fldname),
        }
    }

    fn close(&mut self) -> Result<(), String> {
        self.s1.lock().map_err(|_| "failed to get lock")?.close()?;
        if let Some(s2) = &self.s2 {
            s2.lock().map_err(|_| "failed to get lock")?.close()?;
        }

        Ok(())
    }

    fn to_update_scan(&mut self) -> Result<Arc<Mutex<dyn UpdateScan>>, String> {
        Err("Unexpected cast".to_string())
    }

    fn as_table_scan(&mut self) -> Result<&mut crate::record::table_scan::TableScan, String> {
        Err("Unexpected downcast".to_string())
    }

    fn as_sort_scan(&mut self) -> Result<Arc<Mutex<SortScan>>, String> {
        Err("Unexpected downcast".to_string())
    }
}
