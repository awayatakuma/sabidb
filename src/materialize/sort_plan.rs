use std::{
    cmp::Ordering,
    sync::{Arc, Mutex},
};

use crate::{
    plan::plan::Plan,
    query::{scan::Scan, update_scan::UpdateScan},
    record::schema::Schema,
    tx::transaction::Transaction,
};

use super::{
    materialize_plan::MaterializePlan, record_comparator::RecordComparator, sort_scan::SortScan,
    temp_table::TempTable,
};

pub struct SortPlan {
    tx: Arc<Mutex<Transaction>>,
    p: Arc<Mutex<dyn Plan>>,
    sch: Arc<Mutex<Schema>>,
    comp: RecordComparator,
}

impl SortPlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        p: Arc<Mutex<dyn Plan>>,
        sortfields: Vec<String>,
    ) -> Result<Self, String> {
        let sch = Arc::new(Mutex::new(
            p.lock().map_err(|_| "failed to get lock")?.schema()?,
        ));

        Ok(SortPlan {
            tx: tx,
            p: p,
            sch: sch,
            comp: RecordComparator::new(sortfields),
        })
    }

    fn split_into_runs(&self, mut src: Arc<Mutex<dyn Scan>>) -> Result<Vec<TempTable>, String> {
        let mut temps = Vec::new();
        src.lock()
            .map_err(|_| "failed to get lock")?
            .before_first()?;
        if !src.lock().map_err(|_| "failed to get lock")?.next()? {
            return Ok(temps);
        }

        let mut currenttemp = TempTable::new(self.tx.clone(), self.sch.clone())?;
        temps.push(currenttemp.clone());
        let mut currentscan = currenttemp.open()?;
        while self.copy(&mut src, &mut currentscan)? {
            if self.comp.compare(
                &mut src,
                &mut currentscan
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .to_scan()?,
            )? == Ordering::Less
            {
                currentscan
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .close()?;
                currenttemp = TempTable::new(self.tx.clone(), self.sch.clone())?;
                temps.push(currenttemp.clone());
                currentscan = currenttemp.open()?;
            }
        }
        currentscan
            .lock()
            .map_err(|_| "failed to get lock")?
            .close()?;

        Ok(temps)
    }

    fn do_a_merge_iteration(&self, mut runs: Vec<TempTable>) -> Result<Vec<TempTable>, String> {
        let mut result = Vec::new();
        while runs.len() > 1 {
            let p1 = runs.remove(0);
            let p2 = runs.remove(0);
            result.push(self.merge_two_runs(&p1, &p2)?);
        }
        if runs.len() == 1 {
            result.push(runs.get(0).unwrap().clone());
        }

        Ok(result)
    }

    fn merge_two_runs(&self, p1: &TempTable, p2: &TempTable) -> Result<TempTable, String> {
        let src1 = p1.open()?;
        let src2 = p2.open()?;
        let result = TempTable::new(self.tx.clone(), self.sch.clone())?;
        let mut dest = result.open()?;

        let mut hasmore1 = src1.lock().map_err(|_| "failed to get lock")?.next()?;
        let mut hasmore2 = src2.lock().map_err(|_| "failed to get lock")?.next()?;
        while hasmore1 && hasmore2 {
            if self.comp.compare(
                &src1.lock().map_err(|_| "failed to get lock")?.to_scan()?,
                &src2.lock().map_err(|_| "failed to get lock")?.to_scan()?,
            )? == Ordering::Less
            {
                hasmore1 = self.copy(
                    &mut src1.lock().map_err(|_| "failed to get lock")?.to_scan()?,
                    &mut dest,
                )?;
            } else {
                hasmore2 = self.copy(
                    &mut src2.lock().map_err(|_| "failed to get lock")?.to_scan()?,
                    &mut dest,
                )?;
            }
        }
        if hasmore1 {
            while hasmore1 {
                hasmore1 = self.copy(
                    &mut src1.lock().map_err(|_| "failed to get lock")?.to_scan()?,
                    &mut dest,
                )?;
            }
        } else {
            while hasmore2 {
                hasmore2 = self.copy(
                    &mut src2.lock().map_err(|_| "failed to get lock")?.to_scan()?,
                    &mut dest,
                )?;
            }
        }

        src1.lock().map_err(|_| "failed to get lock")?.close()?;
        src2.lock().map_err(|_| "failed to get lock")?.close()?;
        dest.lock().map_err(|_| "failed to get lock")?.close()?;

        Ok(result)
    }

    fn copy(
        &self,
        src: &mut Arc<Mutex<dyn Scan>>,
        dest: &mut Arc<Mutex<dyn UpdateScan>>,
    ) -> Result<bool, String> {
        dest.lock().map_err(|_| "failed to get lock")?.insert()?;
        for fldname in self
            .sch
            .lock()
            .map_err(|_| "failed to get lock")?
            .fields()
            .lock()
            .map_err(|_| "failed to get lock")?
            .iter()
        {
            dest.lock().map_err(|_| "failed to get lock")?.set_val(
                fldname.clone(),
                src.lock()
                    .map_err(|_| "failed to get lock")?
                    .get_val(fldname)?,
            )?;
        }

        Ok(src.lock().map_err(|_| "failed to get lock")?.next()?)
    }
}

impl Plan for SortPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn crate::query::scan::Scan>>, String> {
        let src = self.p.lock().map_err(|_| "failed to get lock")?.open()?;
        let mut runs = self.split_into_runs(src.clone())?;
        src.lock().map_err(|_| "failed to get lock")?.close()?;
        while runs.len() > 2 {
            runs = self.do_a_merge_iteration(runs)?;
        }

        Ok(Arc::new(Mutex::new(SortScan::new(
            runs,
            self.comp.clone(),
        )?)))
    }

    fn blocks_accessed(&self) -> Result<i32, String> {
        let mp = MaterializePlan::new(self.p.clone(), self.tx.clone());
        mp.blocks_accessed()
    }

    fn records_output(&self) -> Result<i32, String> {
        self.p
            .lock()
            .map_err(|_| "failed to get lock")?
            .records_output()
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        self.p
            .lock()
            .map_err(|_| "failed to get lock")?
            .distinct_values(fldname)
    }

    fn schema(&self) -> Result<Schema, String> {
        Ok(self.sch.lock().map_err(|_| "failed to get lock")?.clone())
    }
}
