use std::sync::{Arc, Mutex};

use crate::{
    index::index::Index,
    query::{constant::Constant, scan::Scan, update_scan::UpdateScan},
    record::{layout::Layout, rid::RID, table_scan::TableScan},
    tx::transaction::Transaction,
};

const NUM_BUCKETS: u64 = 100;

#[derive(Debug, Clone)]
pub struct HashIndex {
    tx: Arc<Mutex<Transaction>>,
    idxname: String,
    layout: Arc<Mutex<Layout>>,
    search_key: Option<Constant>,
    ts: Option<TableScan>,
}

impl Index for HashIndex {
    fn before_first(&mut self, search_key: &Constant) -> Result<(), String> {
        self.close()?;
        self.search_key = Some(search_key.clone());
        let bucket = search_key.hash_code() % NUM_BUCKETS;
        let tblname = format!("{}{}", self.idxname, bucket);
        self.ts = Some(TableScan::new(
            self.tx.clone(),
            tblname,
            self.layout.clone(),
        )?);
        Ok(())
    }

    fn next(&mut self) -> Result<bool, String> {
        while let Some(ref ts) = self.ts {
            if ts
                .get_val(&"dataval".to_string())?
                .eq(&self.search_key.clone().unwrap())
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn get_data_rid(&self) -> Result<RID, String> {
        let blknum = self.ts.as_ref().unwrap().get_int(&"block".to_string())?;
        let id = self.ts.as_ref().unwrap().get_int(&"id".to_string())?;
        let ret = RID::new(blknum, id);

        Ok(ret)
    }

    fn insert(&mut self, dataval: &Constant, datarid: RID) -> Result<(), String> {
        self.before_first(&dataval)?;
        let ts = self.ts.as_mut().unwrap();
        ts.insert()?;
        ts.set_int("block".to_string(), datarid.block_number())?;
        ts.set_int("id".to_string(), datarid.slot())?;
        ts.set_val("dataval".to_string(), dataval.clone())?;

        Ok(())
    }

    fn delete(&mut self, dataval: &Constant, datarid: RID) -> Result<(), String> {
        self.before_first(dataval)?;
        while self.next()? {
            if self.get_data_rid()?.eq(&datarid) {
                self.ts.as_mut().unwrap().delete()?;
                break;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        if let Some(ref mut ts) = self.ts {
            ts.close()?;
        }
        Ok(())
    }
}

impl HashIndex {
    pub fn new(tx: Arc<Mutex<Transaction>>, idxname: String, layout: Arc<Mutex<Layout>>) -> Self {
        HashIndex {
            tx: tx,
            idxname: idxname,
            layout: layout,
            search_key: None,
            ts: None,
        }
    }
}

pub fn search_cost(num_blocks: i32, _rpb: i32) -> i32 {
    num_blocks / (NUM_BUCKETS as i32)
}
