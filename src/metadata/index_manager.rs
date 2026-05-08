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
            table_manager
                .create_table("idxcat".to_string(), sch, tx.clone())?;
        }
        let layout = table_manager
                .get_layout("idxcat".to_string(), tx.clone())?;

        Ok(IndexManager {
            layout: layout,
            table_manager: table_manager,
            stat_manager: stat_manager,
        })
    }

    pub fn create_index(
        &self,
        idxname: String,
        tblname: String,
        fldname: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), String> {
        let mut ts = TableScan::new(tx, "idxcat".to_string(), self.layout.clone())?;
        ts.insert()?;
        ts.set_string("indexname".to_string(), idxname)?;
        ts.set_string("tablename".to_string(), tblname)?;
        ts.set_string("fieldname".to_string(), fldname)?;
        ts.close()?;

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
                let tbl_layout = self.table_manager
                        .get_layout(tblname.clone(), tx.clone())?;
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
