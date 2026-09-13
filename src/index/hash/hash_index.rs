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
    layout: Layout,
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
        let search_key = self
            .search_key
            .clone()
            .ok_or("next called before before_first")?;
        let ts = self.ts.as_mut().ok_or("next called before before_first")?;
        while ts.next()? {
            if ts.get_val(&"dataval".to_string())?.eq(&search_key) {
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
        self.before_first(dataval)?;
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
        if let Some(mut ts) = self.ts.take() {
            ts.close()?;
        }
        Ok(())
    }
}

impl HashIndex {
    pub fn new(tx: Arc<Mutex<Transaction>>, idxname: String, layout: Layout) -> Self {
        HashIndex {
            tx,
            idxname,
            layout,
            search_key: None,
            ts: None,
        }
    }
}

pub fn search_cost(num_blocks: i32, _rpb: i32) -> i32 {
    num_blocks / (NUM_BUCKETS as i32)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::{record::schema::Schema, server::simple_db::SimpleDB};

    fn idx_layout() -> Layout {
        let sch = Schema::new();
        sch.add_int_field(&"block".to_string()).unwrap();
        sch.add_int_field(&"id".to_string()).unwrap();
        sch.add_int_field(&"dataval".to_string()).unwrap();
        Layout::new_from_schema(sch).unwrap()
    }

    /// A key other than `key` that hashes into the same bucket, so a search for
    /// it has to step past the entry stored under `key`.
    fn colliding_key(key: i32) -> i32 {
        let bucket = Constant::new_from_i32(key).hash_code() % NUM_BUCKETS;
        (key + 1..key + 100_000)
            .find(|k| Constant::new_from_i32(*k).hash_code() % NUM_BUCKETS == bucket)
            .expect("no colliding key found")
    }

    fn rids_for(idx: &mut HashIndex, key: i32) -> Vec<RID> {
        idx.before_first(&Constant::new_from_i32(key)).unwrap();
        let mut found = Vec::new();
        while idx.next().unwrap() {
            found.push(idx.get_data_rid().unwrap());
        }
        idx.close().unwrap();
        found
    }

    #[test]
    fn retrieves_only_the_matching_key() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new_with_sizes(temp_dir.path(), 400, 8);
        let mut idx = HashIndex::new(db.new_tx(), "hashidx".to_string(), idx_layout());

        idx.insert(&Constant::new_from_i32(7), RID::new(1, 10))
            .unwrap();
        idx.insert(&Constant::new_from_i32(7), RID::new(2, 20))
            .unwrap();
        idx.insert(&Constant::new_from_i32(8), RID::new(3, 30))
            .unwrap();

        let mut sevens = rids_for(&mut idx, 7);
        sevens.sort_by_key(|r| r.block_number());
        assert_eq!(sevens, vec![RID::new(1, 10), RID::new(2, 20)]);
        assert_eq!(rids_for(&mut idx, 8), vec![RID::new(3, 30)]);
    }

    #[test]
    fn missing_key_terminates() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new_with_sizes(temp_dir.path(), 400, 8);
        let mut idx = HashIndex::new(db.new_tx(), "hashidx".to_string(), idx_layout());

        idx.insert(&Constant::new_from_i32(7), RID::new(1, 10))
            .unwrap();

        assert!(rids_for(&mut idx, colliding_key(7)).is_empty());
    }

    #[test]
    fn delete_removes_only_that_rid() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new_with_sizes(temp_dir.path(), 400, 8);
        let mut idx = HashIndex::new(db.new_tx(), "hashidx".to_string(), idx_layout());

        idx.insert(&Constant::new_from_i32(7), RID::new(1, 10))
            .unwrap();
        idx.insert(&Constant::new_from_i32(7), RID::new(2, 20))
            .unwrap();
        idx.delete(&Constant::new_from_i32(7), RID::new(1, 10))
            .unwrap();

        assert_eq!(rids_for(&mut idx, 7), vec![RID::new(2, 20)]);
    }
}
