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
    fn open(&mut self) -> Result<Box<dyn Scan>, String> {
        Ok(Box::new(
            TableScan::new(self.tx.clone(), self.tblname.clone(), self.layout.clone()).unwrap(),
        ))
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

    fn schema(&self) -> crate::record::schema::Schema {
        self.layout.lock().unwrap().schema().lock().unwrap().clone()
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

#[cfg(test)]
mod tests {

    use tempfile::TempDir;

    use crate::server::simple_db::SimpleDB;

    #[test]
    fn test_scan_1() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new(temp_dir.path());
        // let mdm = db.m

        let tx = db.new_tx();
    }
}
