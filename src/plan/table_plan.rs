use std::sync::{Arc, Mutex};

use crate::{
    metadata::{matadata_manager::MetadataManager, stat_info::StatInfo},
    query::scan::Scan,
    record::{layout::Layout, table_scan::TableScan},
    tx::transaction::Transaction,
};

use super::plan::Plan;

pub struct TablePlan {
    tblname: String,
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Mutex<Layout>>,
    si: StatInfo,
}

impl Plan for TablePlan {
    fn open(&self) -> Result<Arc<Mutex<dyn Scan>>, String> {
        Ok(Arc::new(Mutex::new(
            TableScan::new(self.tx.clone(), self.tblname.clone(), self.layout.clone()).unwrap(),
        )))
    }

    fn blocks_accessed(&self) -> Result<i32, String> {
        Ok(self.si.blocks_accessed())
    }

    fn records_output(&self) -> Result<i32, String> {
        Ok(self.si.records_output())
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        Ok(self.si.distinct_values(fldname))
    }

    fn schema(&self) -> Result<crate::record::schema::Schema, String> {
        Ok(self
            .layout
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()
            .lock()
            .map_err(|_| "failed to get lock")?
            .clone())
    }
}

impl TablePlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        tblname: String,
        md: Arc<Mutex<MetadataManager>>,
    ) -> Result<TablePlan, String> {
        let layout = Arc::new(Mutex::new(
            md.lock()
                .map_err(|_| "failed to get lock")?
                .get_layout(tblname.clone(), tx.clone())?,
        ));
        let si = md.lock().map_err(|_| "failed to get lock")?.get_stat_info(
            tblname.clone(),
            layout.clone(),
            tx.clone(),
        )?;

        Ok(TablePlan {
            tblname: tblname,
            tx: tx,
            layout: layout,
            si: si,
        })
    }
}
