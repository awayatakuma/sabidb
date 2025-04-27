use std::sync::{Arc, Mutex};

use crate::{
    index::index::Index,
    materialize::sort_scan::SortScan,
    query::{scan::Scan, update_scan::UpdateScan},
    record::table_scan::TableScan,
};

pub struct IndexJoinScan {
    lhs: Arc<Mutex<dyn Scan>>,
    idx: Arc<Mutex<dyn Index>>,
    joinfield: String,
    rhs: Arc<Mutex<TableScan>>,
}

impl IndexJoinScan {
    pub(crate) fn new(
        lhs: Arc<Mutex<dyn Scan>>,
        idx: Arc<Mutex<dyn Index>>,
        joinfield: String,
        rhs: Arc<Mutex<TableScan>>,
    ) -> Self {
        IndexJoinScan {
            lhs: lhs,
            idx: idx,
            joinfield: joinfield,
            rhs: rhs,
        }
    }

    fn reset_index(&mut self) -> Result<(), String> {
        let searchkey = self
            .lhs
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_val(&self.joinfield)?;
        self.idx
            .lock()
            .map_err(|_| "failed to get lock")?
            .before_first(&searchkey)?;
        Ok(())
    }
}

impl Scan for IndexJoinScan {
    fn before_first(&mut self) -> Result<(), String> {
        let mut lhs = self.lhs.lock().map_err(|_| "failed to get lock")?;
        lhs.before_first()?;
        lhs.next()?;
        drop(lhs);
        self.reset_index()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool, String> {
        loop {
            if self.idx.lock().map_err(|_| "failed to get lock")?.next()? {
                self.rhs
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .move_to_rid(
                        self.idx
                            .lock()
                            .map_err(|_| "failed to get lock")?
                            .get_data_rid()?,
                    )?;
                return Ok(true);
            }
            if !self.lhs.lock().map_err(|_| "failed to get lock")?.next()? {
                return Ok(false);
            }
            self.reset_index()?;
        }
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        if self
            .rhs
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?
        {
            self.rhs
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_int(fldname)
        } else {
            self.lhs
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_int(fldname)
        }
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        if self
            .rhs
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?
        {
            self.rhs
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_string(fldname)
        } else {
            self.lhs
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_string(fldname)
        }
    }

    fn get_val(&self, fldname: &String) -> Result<crate::query::constant::Constant, String> {
        if self
            .rhs
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?
        {
            self.rhs
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(fldname)
        } else {
            self.lhs
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(fldname)
        }
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        Ok(self
            .rhs
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?
            || self
                .lhs
                .lock()
                .map_err(|_| "failed to get lock")?
                .has_field(fldname)?)
    }

    fn close(&mut self) -> Result<(), String> {
        self.lhs.lock().map_err(|_| "failed to get lock")?.close()?;
        self.idx.lock().map_err(|_| "failed to get lock")?.close()?;
        self.rhs.lock().map_err(|_| "failed to get lock")?.close()?;
        Ok(())
    }

    fn to_update_scan(&mut self) -> Result<Arc<Mutex<(dyn UpdateScan + 'static)>>, String> {
        Err("Unexpected downcast".to_string())
    }

    fn as_table_scan(&mut self) -> Result<&mut TableScan, String> {
        Err("Unexpected downcast".to_string())
    }

    fn as_sort_scan(&mut self) -> Result<Arc<Mutex<SortScan>>, String> {
        Err("Unexpected downcast".to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::TempDir;

    use crate::{
        index::planner::index_join_plan::IndexJoinPlan,
        plan::{plan::Plan, table_plan::TablePlan},
        query::{scan::Scan, update_scan::UpdateScan},
        server::simple_db::SimpleDB,
        testlib::helper::create_student_data,
    };

    #[test]
    fn test_index_join() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SimpleDB::new(temp_dir.path());
        create_student_data(&mut db);

        let tx = db.new_tx();
        let mdm = db.metadata_manager();

        let indexes = mdm
            .lock()
            .unwrap()
            .get_index_info("enrolls".to_string(), tx.clone())
            .unwrap();

        let sididx = indexes.get("studentid").unwrap().clone();

        let studentplan = TablePlan::new(tx.clone(), "students".to_string(), mdm.clone()).unwrap();
        let enrollplan = TablePlan::new(tx.clone(), "enrolls".to_string(), mdm.clone()).unwrap();

        // Two different ways to use the index in simpledb:
        // useIndexManually
        let s1 = studentplan.open().unwrap();
        let binding = enrollplan.open().unwrap();
        let mut binding = binding.lock().unwrap();
        let s2 = binding.as_table_scan().unwrap();
        let idx = sididx.open().unwrap();

        while s1.lock().unwrap().next().unwrap() {
            let c = s1.lock().unwrap().get_val(&"sid".to_string()).unwrap();
            idx.lock().unwrap().before_first(&c).unwrap();
            while idx.lock().unwrap().next().unwrap() {
                let datarid = idx.lock().unwrap().get_data_rid().unwrap();
                s2.move_to_rid(datarid).unwrap();
                println!("{}", s2.get_string(&"grade".to_string()).unwrap())
            }
        }
        idx.lock().unwrap().close().unwrap();
        s1.lock().unwrap().close().unwrap();
        s2.close().unwrap();

        // useIndexScan
        // Open an index join scan on the table.
        let idxplan = IndexJoinPlan::new(
            Arc::new(Mutex::new(studentplan)),
            Arc::new(Mutex::new(enrollplan)),
            sididx,
            "sid".to_string(),
        )
        .unwrap();

        let s = idxplan.open().unwrap();
        s.lock().unwrap().before_first().unwrap();
        while s.lock().unwrap().next().unwrap() {
            println!(
                "{}",
                s.lock().unwrap().get_string(&"grade".to_string()).unwrap()
            );
        }
        s.lock().unwrap().close().unwrap();
        tx.lock().unwrap().commit().unwrap();
    }
}
