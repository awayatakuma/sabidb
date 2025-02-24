use std::sync::{Arc, Mutex};

use crate::{
    query::{scan::Scan, update_scan::UpdateScan},
    record::{schema::Schema, table_scan::TableScan},
    tx::transaction::Transaction,
};

use super::table_manager::{TableManager, MAX_NAME};

const MAX_VIEWDEF: i32 = 100;

#[derive(Debug, Clone)]
pub struct ViewManager {
    table_manager: Arc<Mutex<TableManager>>,
}

impl ViewManager {
    pub fn new(
        is_new: bool,
        tbl_manager: Arc<Mutex<TableManager>>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Self, String> {
        if is_new {
            let mut tbl_mgr = tbl_manager.lock().map_err(|_| "failed to get lock")?;
            let mut sch = Schema::new();
            sch.add_string_field(&"viewname".to_string(), MAX_NAME)?;
            sch.add_string_field(&"viewdef".to_string(), MAX_VIEWDEF)?;
            let sch = Arc::new(Mutex::new(sch));
            tbl_mgr.create_table("viewcat".to_string(), sch, tx.clone())?;
        }

        Ok(ViewManager {
            table_manager: tbl_manager,
        })
    }

    pub fn create_view(
        &self,
        vname: String,
        vdef: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), String> {
        let layout = Arc::new(Mutex::new(
            self.table_manager
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_layout("viewcat".to_string(), tx.clone())?,
        ));
        let mut ts = TableScan::new(tx.clone(), "viewcat".to_string(), layout)?;

        ts.insert()?;
        ts.set_string("viewname".to_string(), vname)?;
        ts.set_string("viewdef".to_string(), vdef)?;
        ts.close()?;
        Ok(())
    }

    pub fn get_view_def(
        &self,
        vname: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Option<String>, String> {
        let mut ret = None;
        let layout = Arc::new(Mutex::new(
            self.table_manager
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_layout("viewcat".to_string(), tx.clone())?,
        ));
        let mut ts = TableScan::new(tx.clone(), "viewcat".to_string(), layout)?;

        while ts.next()? {
            if ts.get_string(&"viewname".to_string())?.eq(&vname) {
                ret = Some(ts.get_string(&"viewdef".to_string())?);
                break;
            }
        }
        ts.close()?;

        Ok(ret)
    }
}
