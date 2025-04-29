use std::sync::{Arc, Mutex};

use lazy_static::lazy_static;

use crate::{
    query::{scan::Scan, update_scan::UpdateScan},
    record::{layout::Layout, schema::Schema, table_scan::TableScan},
    tx::transaction::Transaction,
};

lazy_static! {
    static ref next_table_num: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));
}

#[derive(Clone)]
pub struct TempTable {
    tx: Arc<Mutex<Transaction>>,
    tblname: String,
    layout: Arc<Mutex<Layout>>,
}

impl TempTable {
    pub fn new(tx: Arc<Mutex<Transaction>>, sch: Arc<Mutex<Schema>>) -> Result<Self, String> {
        Ok(TempTable {
            tx: tx,
            tblname: next_table_name(),
            layout: Arc::new(Mutex::new(Layout::new_from_schema(sch)?)),
        })
    }

    pub fn open(&self) -> Result<Arc<Mutex<(dyn UpdateScan + 'static)>>, String> {
        let mut s = TableScan::new(self.tx.clone(), self.tblname.clone(), self.layout.clone())?;
        s.to_update_scan()
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn get_layout(&self) -> Arc<Mutex<Layout>> {
        self.layout.clone()
    }
}

fn next_table_name() -> String {
    *next_table_num.lock().unwrap() += 1;
    let next_num = *next_table_num.lock().unwrap();

    format!("temp{}", next_num)
}
