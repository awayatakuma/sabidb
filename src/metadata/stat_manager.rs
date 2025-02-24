use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    query::{scan::Scan, update_scan::UpdateScan},
    record::{layout::Layout, table_scan::TableScan},
    tx::transaction::Transaction,
};

use super::{stat_info::StatInfo, table_manager::TableManager};

#[derive(Debug, Clone)]
pub struct StatManager {
    table_manager: Arc<Mutex<TableManager>>,
    table_stats: Arc<Mutex<HashMap<String, StatInfo>>>,
    num_calls: Arc<Mutex<i32>>,
}

impl StatManager {
    pub fn new(
        table_manager: Arc<Mutex<TableManager>>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Self, String> {
        let mut ret = StatManager {
            table_manager: table_manager,
            table_stats: Arc::new(Mutex::new(HashMap::new())),
            num_calls: Arc::new(Mutex::new(0)),
        };
        ret.refreash_statistics(tx.clone())?;
        Ok(ret)
    }

    pub fn get_stat_info(
        &mut self,
        tblname: String,
        layout: Arc<Mutex<Layout>>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo, String> {
        let num_calls = {
            let mut num_calls = self.num_calls.lock().map_err(|_| "failed to get lock")?;
            *num_calls += 1;
            *num_calls
        };
        if num_calls > 100 {
            self.refreash_statistics(tx.clone())?;
        }
        let mut table_stats = self.table_stats.lock().map_err(|_| "failed to get lock")?;
        if let Some(si) = table_stats.get(&tblname) {
            Ok(si.clone())
        } else {
            let si = Self::calc_table_stats(tblname.clone(), layout, tx)?;
            table_stats.insert(tblname, si.clone());
            Ok(si)
        }
    }

    fn refreash_statistics(&mut self, tx: Arc<Mutex<Transaction>>) -> Result<(), String> {
        let table_manager = self
            .table_manager
            .lock()
            .map_err(|_| "failed to get lock")?;
        let tcatlayout = Arc::new(Mutex::new(
            table_manager.get_layout("tblcat".to_string(), tx.clone())?,
        ));
        let mut tcat = TableScan::new(tx.clone(), "tblcat".to_string(), tcatlayout)?;
        let mut table_stats = self.table_stats.lock().map_err(|_| "failed to get lock")?;
        while tcat.next()? {
            let tblname = tcat.get_string(&"tblname".to_string())?;
            let layout = Arc::new(Mutex::new(
                table_manager.get_layout(tblname.clone(), tx.clone())?,
            ));
            let si = Self::calc_table_stats(tblname.clone(), layout, tx.clone())?;
            table_stats.insert(tblname.clone(), si);
        }
        tcat.close()?;

        Ok(())
    }
    fn calc_table_stats(
        tblname: String,
        layout: Arc<Mutex<Layout>>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<StatInfo, String> {
        let mut num_recs = 0;
        let mut num_blocks = 0;
        let mut ts = TableScan::new(tx.clone(), tblname, layout)?;
        while ts.next()? {
            num_recs += 1;
            num_blocks = ts.get_rid()?.block_number() + 1;
        }
        ts.close()?;
        let ret = StatInfo::new(num_blocks, num_recs);
        Ok(ret)
    }
}
