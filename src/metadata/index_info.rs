use std::sync::{Arc, Mutex};

use crate::{
    index::{
        btree::btree_index::{self, BTreeIndex},
        index::Index,
    },
    record::{
        layout::Layout,
        schema::{
            field_type::{INTEGER, VARCHAR},
            Schema,
        },
    },
    tx::transaction::Transaction,
};

use super::stat_info::StatInfo;

#[derive(Debug, Clone)]
pub struct IndexInfo {
    idxname: String,
    fldname: String,
    tx: Arc<Mutex<Transaction>>,
    tbl_schema: Arc<Mutex<Schema>>,
    idx_layout: Option<Arc<Mutex<Layout>>>,
    si: StatInfo,
}

impl IndexInfo {
    pub fn new(
        idxname: String,
        fldname: String,
        tbl_schema: Arc<Mutex<Schema>>,
        tx: Arc<Mutex<Transaction>>,
        si: StatInfo,
    ) -> Result<Self, String> {
        let mut ret = IndexInfo {
            idxname: idxname,
            fldname: fldname,
            tx: tx,
            tbl_schema: tbl_schema,
            idx_layout: None,
            si: si,
        };
        ret.idx_layout = Some(Arc::new(Mutex::new(ret.create_idx_layout()?)));

        Ok(ret)
    }

    pub(crate) fn open(&self) -> Result<Arc<Mutex<dyn Index>>, String> {
        Ok(Arc::new(Mutex::new(BTreeIndex::new(
            self.tx.clone(),
            self.idxname.clone(),
            self.idx_layout.as_ref().unwrap().clone(),
        )?)))
        // Arc::new(Mutex::new(HashIndex::new(
        //     self.tx.clone(),
        //     self.idxname.clone(),
        //     self.idx_layout.as_ref().unwrap().clone(),
        // )))
    }

    pub fn blocks_accessed(&self) -> Result<i32, String> {
        let rpb = self
            .tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .block_size()?
            / self
                .idx_layout
                .as_ref()
                .unwrap()
                .lock()
                .map_err(|_| "failed to get lock")?
                .slot_size();
        let num_blocks = self.si.records_output() / rpb;
        // Ok(hash_index::search_cost(num_blocks, rpb))
        Ok(btree_index::search_cost(num_blocks, rpb))
    }

    pub fn records_output(&self) -> i32 {
        self.si.records_output() / self.si.distinct_values(self.fldname.clone())
    }

    pub fn distinct_values(&self, fname: String) -> i32 {
        if self.fldname.eq(&fname) {
            1
        } else {
            self.si.distinct_values(self.fldname.clone())
        }
    }

    fn create_idx_layout(&self) -> Result<Layout, String> {
        let mut sch = Schema::new();
        sch.add_int_field(&"block".to_string())?;
        sch.add_int_field(&"id".to_string())?;
        let tbl_schema = self.tbl_schema.lock().map_err(|_| "failed to get lock")?;
        if tbl_schema.field_type(&self.fldname)? == INTEGER {
            sch.add_int_field(&"dataval".to_string())?;
        } else if tbl_schema.field_type(&self.fldname)? == VARCHAR {
            let fldlen = tbl_schema.length(&self.fldname)?;
            sch.add_string_field(&"dataval".to_string(), fldlen)?;
        } else {
            panic!("an unexpected type");
        }
        let ret = Layout::new_from_schema(Arc::new(Mutex::new(sch)))?;
        Ok(ret)
    }
}
