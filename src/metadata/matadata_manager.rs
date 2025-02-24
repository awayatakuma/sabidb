use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    record::{layout::Layout, schema::Schema},
    tx::transaction::Transaction,
};

use super::{
    index_info::IndexInfo, index_manager::IndexManager, stat_info::StatInfo,
    stat_manager::StatManager, table_manager::TableManager, view_manager::ViewManager,
};

#[derive(Debug, Clone)]
pub struct MetadataManager {
    tbl_manager: Arc<Mutex<TableManager>>,
    view_manager: Arc<Mutex<ViewManager>>,
    stat_manager: Arc<Mutex<StatManager>>,
    idx_manager: Arc<Mutex<IndexManager>>,
}

impl MetadataManager {
    pub fn new(is_new: bool, tx: Arc<Mutex<Transaction>>) -> Result<Self, String> {
        let tbl_manager = Arc::new(Mutex::new(TableManager::new(is_new, tx.clone())?));
        let view_manager = Arc::new(Mutex::new(ViewManager::new(
            is_new,
            tbl_manager.clone(),
            tx.clone(),
        )?));
        let stat_manager = Arc::new(Mutex::new(StatManager::new(
            tbl_manager.clone(),
            tx.clone(),
        )?));
        let idx_manager = Arc::new(Mutex::new(IndexManager::new(
            is_new,
            tbl_manager.clone(),
            stat_manager.clone(),
            tx.clone(),
        )?));
        Ok(MetadataManager {
            tbl_manager: tbl_manager,
            view_manager: view_manager,
            stat_manager: stat_manager,
            idx_manager: idx_manager,
        })
    }

    pub fn create_table(
        &mut self,
        tblname: String,
        sch: Arc<Mutex<Schema>>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), String> {
        self.tbl_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .create_table(tblname, sch.clone(), tx.clone())?;
        Ok(())
    }

    pub fn get_layout(
        &self,
        tblname: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Layout, String> {
        let ret = self
            .tbl_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_layout(tblname, tx)?;
        Ok(ret)
    }

    pub fn create_view(
        &mut self,
        viewname: String,
        viewdef: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), String> {
        self.view_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .create_view(viewname, viewdef, tx.clone())?;
        Ok(())
    }

    pub fn get_view_def(
        &self,
        viewname: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Option<String>, String> {
        let ret = self
            .view_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_view_def(viewname, tx)?;
        Ok(ret)
    }

    pub fn create_index(
        &mut self,
        idxname: String,
        tblname: String,
        fldname: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), String> {
        self.idx_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .create_index(idxname, tblname, fldname, tx.clone())?;
        Ok(())
    }

    pub fn get_index_info(
        &self,
        tblname: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<HashMap<String, IndexInfo>, String> {
        let ret = self
            .idx_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_index_info(tblname, tx)?;
        Ok(ret)
    }

    pub fn get_stat_info(
        &self,
        tblname: String,
        layout: Arc<Mutex<Layout>>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo, String> {
        let ret = self
            .stat_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_stat_info(tblname, layout, tx)?;
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rand::Rng;
    use tempfile::TempDir;

    use crate::{
        metadata::matadata_manager::MetadataManager,
        query::update_scan::UpdateScan,
        record::{
            schema::{
                field_type::{INTEGER, VARCHAR},
                Schema,
            },
            table_scan::TableScan,
        },
        server::simple_db::SimpleDB,
    };

    #[test]
    fn test_metadata_manager() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(SimpleDB::new(temp_dir.path(), 400, 8));
        let tx = db.new_tx();
        let mut mdm = MetadataManager::new(true, tx.clone()).unwrap();

        let mut sch = Schema::new();
        sch.add_int_field(&"A".to_string()).unwrap();
        sch.add_string_field(&"B".to_string(), 9).unwrap();

        let sch = Arc::new(Mutex::new(sch));
        mdm.create_table("MyTable".to_string(), sch, tx.clone())
            .unwrap();
        let layout = mdm.get_layout("MyTable".to_string(), tx.clone()).unwrap();

        // Part 1: Table Metadata
        {
            let size = layout.slot_size();
            let binding = layout.schema();
            let sch2 = binding.lock().unwrap();
            println!("MyTable has slot size {}", size);
            assert!(size == 21);
            println!("Its fields are:");
            let binding = sch2.fields();
            let binding = binding.lock().unwrap();
            let fldnames = binding.iter();
            for fldname in fldnames {
                if fldname.eq("A") {
                    println!("A : int");
                    assert_eq!(sch2.field_type(fldname), Ok(INTEGER));
                } else if fldname.eq("B") {
                    let strlen = sch2.length(fldname).unwrap();
                    println!("B : varchar({})", strlen);
                    assert_eq!(sch2.field_type(fldname), Ok(VARCHAR));
                    assert_eq!(strlen, 9);
                } else {
                    panic!("unreachable!!");
                }
            }
        }

        // Part 2: Statistics Metadata
        {
            let layout = Arc::new(Mutex::new(layout));
            let mut ts = TableScan::new(tx.clone(), "MyTable".to_string(), layout.clone()).unwrap();
            let mut rng = rand::rng();

            for _ in 0..50 {
                ts.insert().unwrap();
                let n = rng.random_range(0..=50);
                ts.set_int("A".to_string(), n).unwrap();
                ts.set_string("B".to_string(), format!("rec{}", n)).unwrap();
            }
            let si = mdm
                .get_stat_info("MyTable".to_string(), layout, tx.clone())
                .unwrap();
            let bl = si.blocks_accessed();
            println!("B(MyTable) = {}", bl);
            assert_eq!(bl, 3);
            let ro = si.records_output();
            println!("R(MyTable) = {}", ro);
            assert_eq!(ro, 50);
            let dva = si.distinct_values("A".to_string());
            println!("V(MyTable,A) = {}", dva);
            assert_eq!(dva, 17);
            let dvb = si.distinct_values("B".to_string());
            println!("V(MyTable,B) = {}", dvb);
            assert_eq!(dvb, 17);
            let bl = si.blocks_accessed();
            println!("B(MyTable) = {}", bl);
            assert_eq!(bl, 3);
            let ro = si.records_output();
            println!("R(MyTable) = {}", ro);
            assert_eq!(ro, 50);
            let dva = si.distinct_values("A".to_string());
            println!("V(MyTable,A) = {}", dva);
            assert_eq!(dva, 17);
            let dvb = si.distinct_values("B".to_string());
            println!("V(MyTable,B) = {}", dvb);
            assert_eq!(dvb, 17);
        }

        // Part 3: View Metadata
        let viewdef = "select B from MyTable where A = 1";
        mdm.create_view("viewA".to_string(), viewdef.to_string(), tx.clone())
            .unwrap();
        let v = mdm
            .get_view_def("viewA".to_string(), tx.clone())
            .unwrap()
            .unwrap();
        println!("View def = {}", v);
        assert_eq!(viewdef, v);

        // Part 4: Index Metadata
        mdm.create_index(
            "indexA".to_string(),
            "MyTable".to_string(),
            "A".to_string(),
            tx.clone(),
        )
        .unwrap();

        mdm.create_index(
            "indexB".to_string(),
            "MyTable".to_string(),
            "B".to_string(),
            tx.clone(),
        )
        .unwrap();

        let idxmap = mdm
            .get_index_info("MyTable".to_string(), tx.clone())
            .unwrap();
        let ii = idxmap.get("A").unwrap().clone();

        let bl = ii.blocks_accessed().unwrap();
        println!("B(indexA) = {}", bl);
        assert_eq!(bl, 0);
        let ro = ii.records_output();
        println!("R(indexA) = {}", ro);
        assert_eq!(ro, 2);
        let dva = ii.distinct_values("A".to_string());
        println!("V(indexA,A) = {}", dva);
        assert_eq!(dva, 1);
        let dvb = ii.distinct_values("B".to_string());
        println!("V(indexA,B) = {}", dvb);
        assert_eq!(dvb, 17);

        let ii = idxmap.get("B").unwrap().clone();
        let bl = ii.blocks_accessed().unwrap();
        println!("B(indexB) = {}", bl);
        assert_eq!(bl, 0);
        let ro = ii.records_output();
        println!("R(indexB) = {}", ro);
        assert_eq!(ro, 2);
        let dva = ii.distinct_values("A".to_string());
        println!("V(indexB,A) = {}", dva);
        assert_eq!(dva, 17);
        let dvb = ii.distinct_values("B".to_string());
        println!("V(indexB,B) = {}", dvb);
        assert_eq!(dvb, 1);

        tx.lock().unwrap().commit().unwrap();
    }
}
