use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    query::{scan::Scan, update_scan::UpdateScan},
    record::{layout::Layout, schema::Schema, table_scan::TableScan},
    tx::transaction::Transaction,
};

use super::{
    index_info::IndexInfo,
    stat_manager::StatManager,
    table_manager::{TableManager, MAX_NAME},
};

#[derive(Debug, Clone)]
pub struct IndexManager {
    layout: Layout,
    table_manager: Arc<TableManager>,
    stat_manager: Arc<Mutex<StatManager>>,
}

impl IndexManager {
    pub fn new(
        is_new: bool,
        table_manager: Arc<TableManager>,
        stat_manager: Arc<Mutex<StatManager>>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Self, String> {
        if is_new {
            let sch = Schema::new();
            sch.add_string_field(&"indexname".to_string(), MAX_NAME)?;
            sch.add_string_field(&"tablename".to_string(), MAX_NAME)?;
            sch.add_string_field(&"fieldname".to_string(), MAX_NAME)?;
            table_manager.create_table("idxcat".to_string(), sch, tx.clone())?;
        }
        let layout = table_manager.get_layout("idxcat".to_string(), tx.clone())?;

        Ok(IndexManager {
            layout,
            table_manager,
            stat_manager,
        })
    }

    pub fn create_index(
        &self,
        idxname: String,
        tblname: String,
        fldname: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), String> {
        let mut ts = TableScan::new(tx.clone(), "idxcat".to_string(), self.layout.clone())?;
        ts.insert()?;
        ts.set_string("indexname".to_string(), idxname.clone())?;
        ts.set_string("tablename".to_string(), tblname.clone())?;
        ts.set_string("fieldname".to_string(), fldname.clone())?;
        ts.close()?;

        self.index_existing_records(idxname, tblname, fldname, tx)
    }

    fn index_existing_records(
        &self,
        idxname: String,
        tblname: String,
        fldname: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), String> {
        let tbl_layout = self.table_manager.get_layout(tblname.clone(), tx.clone())?;
        let tblsi = self
            .stat_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_stat_info(tblname.clone(), tbl_layout.clone(), tx.clone())?;
        let ii = IndexInfo::new(
            idxname,
            fldname.clone(),
            tbl_layout.schema(),
            tx.clone(),
            tblsi,
        )?;
        let idx = ii.open()?;

        let mut ts = TableScan::new(tx, tblname, tbl_layout)?;
        while ts.next()? {
            let dataval = ts.get_val(&fldname)?;
            let rid = ts.get_rid()?;
            idx.lock()
                .map_err(|_| "failed to get lock")?
                .insert(&dataval, rid)?;
        }
        ts.close()?;
        idx.lock().map_err(|_| "failed to get lock")?.close()?;

        Ok(())
    }

    pub fn get_index_info(
        &self,
        tblname: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<HashMap<String, IndexInfo>, String> {
        let mut ret = HashMap::new();
        let mut ts = TableScan::new(tx.clone(), "idxcat".to_string(), self.layout.clone())?;
        while ts.next()? {
            if ts.get_string(&"tablename".to_string())?.eq(&tblname) {
                let idxname = ts.get_string(&"indexname".to_string())?;
                let fldname = ts.get_string(&"fieldname".to_string())?;
                let tbl_layout = self.table_manager.get_layout(tblname.clone(), tx.clone())?;
                let tblsi = self
                    .stat_manager
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .get_stat_info(tblname.clone(), tbl_layout.clone(), tx.clone())?;
                let sch = tbl_layout.schema();
                let ii = IndexInfo::new(idxname, fldname.clone(), sch, tx.clone(), tblsi)?;
                ret.insert(fldname, ii);
            }
        }
        ts.close()?;

        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use crate::{
        plan::planner::Planner, server::simple_db::SimpleDB, tx::transaction::Transaction,
    };
    use std::sync::{Arc, Mutex};

    fn names_matching(planner: &mut Planner, tx: Arc<Mutex<Transaction>>, key: i32) -> Vec<String> {
        let qry = format!("select a, b from T where a = {}", key);
        let plan = planner.create_query_planner(&qry, tx).unwrap();
        let scan = plan.lock().unwrap().open().unwrap();
        let mut found = Vec::new();
        while scan.lock().unwrap().next().unwrap() {
            found.push(scan.lock().unwrap().get_string(&"b".to_string()).unwrap());
        }
        scan.lock().unwrap().close().unwrap();
        found.sort();
        found
    }

    #[test]
    fn index_created_after_inserts_sees_the_existing_rows() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new_with_refined_planners(temp_dir.path());
        let tx = db.new_tx();
        let mut planner = db.planner.clone().unwrap();

        planner
            .execute_update("create table T(a int, b varchar(9))", tx.clone())
            .unwrap();
        for i in 0..20 {
            let cmd = format!("insert into T(a, b) values ({}, 'old{}')", i, i);
            planner.execute_update(&cmd, tx.clone()).unwrap();
        }

        planner
            .execute_update("create index aidx on T(a)", tx.clone())
            .unwrap();
        assert_eq!(names_matching(&mut planner, tx.clone(), 5), vec!["old5"]);

        planner
            .execute_update("insert into T(a, b) values (5, 'new')", tx.clone())
            .unwrap();
        assert_eq!(
            names_matching(&mut planner, tx.clone(), 5),
            vec!["new", "old5"]
        );

        assert!(names_matching(&mut planner, tx.clone(), 99).is_empty());
        tx.lock().unwrap().commit().unwrap();
    }
}
