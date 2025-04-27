use std::sync::{Arc, Mutex};

use crate::{
    index::index::Index,
    materialize::sort_scan::SortScan,
    query::{constant::Constant, scan::Scan, update_scan::UpdateScan},
    record::table_scan::TableScan,
};

pub struct IndexSelectScan {
    ts: TableScan,
    idx: Arc<Mutex<dyn Index>>,
    val: Constant,
}

impl IndexSelectScan {
    pub(crate) fn new(ts: TableScan, idx: Arc<Mutex<dyn Index>>, val: Constant) -> Self {
        IndexSelectScan {
            ts: ts,
            idx: idx,
            val: val,
        }
    }
}

impl Scan for IndexSelectScan {
    fn before_first(&mut self) -> Result<(), String> {
        self.idx
            .lock()
            .map_err(|_| "failed to get lock")?
            .before_first(&self.val)
    }

    fn next(&mut self) -> Result<bool, String> {
        let ok = self.idx.lock().map_err(|_| "failed to get lock")?.next()?;
        if ok {
            let rid = self
                .idx
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_data_rid()?;
            self.ts.move_to_rid(rid)?;
        }
        return Ok(ok);
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        self.ts.get_int(fldname)
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        self.ts.get_string(fldname)
    }

    fn get_val(&self, fldname: &String) -> Result<Constant, String> {
        self.ts.get_val(fldname)
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        self.ts.has_field(fldname)
    }

    fn close(&mut self) -> Result<(), String> {
        self.idx.lock().map_err(|_| "failed to get lock")?.close()?;
        self.ts.close()?;
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
        index::planner::index_select_plan::IndexSelectPlan,
        plan::{plan::Plan, table_plan::TablePlan},
        query::{constant::Constant, scan::Scan, update_scan::UpdateScan},
        server::simple_db::SimpleDB,
        testlib::helper::create_student_data,
    };

    #[test]
    fn test_index_select() {
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

        let enrollplan = TablePlan::new(tx.clone(), "enrolls".to_string(), mdm.clone()).unwrap();
        let c = Constant::new_from_i32(6);

        // Two different ways to use the index in simpledb:
        // useIndexManually
        let binding = enrollplan.open().unwrap();
        let mut binding = binding.lock().unwrap();
        let s = binding.as_table_scan().unwrap();
        let idx = sididx.open().unwrap();
        idx.lock().unwrap().before_first(&c).unwrap();
        while idx.lock().unwrap().next().unwrap() {
            let datarid = idx.lock().unwrap().get_data_rid().unwrap();
            s.move_to_rid(datarid).unwrap();
            println!("{}", s.get_string(&"grade".to_string()).unwrap())
        }
        idx.lock().unwrap().close().unwrap();
        s.close().unwrap();

        // useIndexScan
        // Open an index join scan on the table.
        let idxplan = IndexSelectPlan::new(Arc::new(Mutex::new(enrollplan)), sididx, c);

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
