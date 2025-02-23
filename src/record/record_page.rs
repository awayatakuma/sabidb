use std::sync::{Arc, Mutex};

use crate::{file::block_id::BlockId, tx::transaction::Transaction};

use super::{layout::Layout, schema::field_type::INTEGER};

pub const EMPTY: i32 = 0;
pub const USED: i32 = 1;

#[derive(Clone, Debug)]
pub struct RecordPage {
    tx: Arc<Mutex<Transaction>>,
    blk: BlockId,
    layout: Arc<Mutex<Layout>>,
}

impl RecordPage {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        blk: BlockId,
        layout: Arc<Mutex<Layout>>,
    ) -> Result<Self, String> {
        tx.lock().map_err(|_| "failed to get lock")?.pin(&blk)?;
        Ok(RecordPage {
            tx: tx,
            blk: blk,
            layout: layout,
        })
    }

    pub fn get_int(&self, slot: i32, fldname: String) -> Result<i32, String> {
        let fldpos = self.offset(slot)?
            + self
                .layout
                .lock()
                .map_err(|_| "failed to get lock")?
                .offset(&fldname)?;

        let ret = self
            .tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_int(&self.blk, fldpos)?;

        Ok(ret)
    }

    pub fn get_string(&self, slot: i32, fldname: String) -> Result<String, String> {
        let fldpos = self.offset(slot)?
            + self
                .layout
                .lock()
                .map_err(|_| "failed to get lock")?
                .offset(&fldname)?;

        let ret = self
            .tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_string(&self.blk, fldpos)?;

        Ok(ret)
    }

    pub fn set_int(&mut self, slot: i32, fldname: String, val: i32) -> Result<(), String> {
        let fldpos = self.offset(slot)?
            + self
                .layout
                .lock()
                .map_err(|_| "failed to get lock")?
                .offset(&fldname)?;

        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .set_int(&self.blk, fldpos, val, true)?;

        Ok(())
    }

    pub fn set_string(&mut self, slot: i32, fldname: String, val: String) -> Result<(), String> {
        let fldpos = self.offset(slot)?
            + self
                .layout
                .lock()
                .map_err(|_| "failed to get lock")?
                .offset(&fldname)?;

        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .set_string(&self.blk, fldpos, val, true)?;

        Ok(())
    }

    pub fn delete(&mut self, slot: i32) -> Result<(), String> {
        self.set_flag(slot, EMPTY)
    }

    pub fn format(&mut self) -> Result<(), String> {
        let mut slot = 0;

        while self.is_valid_slot(slot)? {
            let tx = self.tx.lock().map_err(|_| "failed to get lock")?;
            tx.set_int(&self.blk, self.offset(slot)?, EMPTY, false)?;
            let fldnames = self
                .layout
                .lock()
                .map_err(|_| "failed to get lock")?
                .schema()
                .lock()
                .map_err(|_| "failed to get lock")?
                .fields()
                .lock()
                .map_err(|_| "failed to get lock")?
                .clone();

            for fldname in fldnames.iter() {
                let fldpos = self.offset(slot)?
                    + self
                        .layout
                        .lock()
                        .map_err(|_| "failed to get lock")?
                        .offset(fldname)?;
                if self
                    .layout
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .schema()
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .field_type(fldname)?
                    == INTEGER
                {
                    tx.set_int(&self.blk, fldpos, 0, false)?;
                } else {
                    tx.set_string(&self.blk, fldpos, "".to_string(), false)?;
                }
            }
            slot += 1;
        }

        Ok(())
    }

    pub fn next_after(&self, slot: i32) -> Result<i32, String> {
        self.search_after(slot, USED)
    }

    pub fn insert_after(&mut self, slot: i32) -> Result<i32, String> {
        let newslot = self.search_after(slot, EMPTY)?;
        if newslot >= 0 {
            self.set_flag(newslot, USED)?;
        }
        Ok(newslot)
    }

    pub fn block(&self) -> BlockId {
        self.blk.clone()
    }

    fn set_flag(&mut self, slot: i32, flag: i32) -> Result<(), String> {
        self.tx.lock().map_err(|_| "failed to get lock")?.set_int(
            &self.blk,
            self.offset(slot)? as usize,
            flag,
            true,
        )
    }

    fn search_after(&self, slot: i32, flag: i32) -> Result<i32, String> {
        let mut slot = slot + 1;
        while self.is_valid_slot(slot)? {
            if self
                .tx
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_int(&self.blk, self.offset(slot)? as usize)?
                == flag
            {
                return Ok(slot);
            }
            slot += 1;
        }
        Ok(-1)
    }

    fn is_valid_slot(&self, slot: i32) -> Result<bool, String> {
        Ok(self.offset(slot + 1)? as i32
            <= self
                .tx
                .lock()
                .map_err(|_| "failed to get lock")?
                .block_size()?)
    }

    fn offset(&self, slot: i32) -> Result<usize, String> {
        let ret = slot
            * self
                .layout
                .lock()
                .map_err(|_| "failed to get lock")?
                .slot_size();
        Ok(ret as usize)
    }
}
#[cfg(test)]
mod tests {

    use std::sync::{Arc, Mutex};

    use tempfile::TempDir;

    use crate::{record::schema::Schema, server::simple_db::SimpleDB};

    use super::{Layout, RecordPage};

    use rand::prelude::*;

    #[test]
    fn test_record_page() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(SimpleDB::new(temp_dir.path(), 400, 8));
        let tx = db.new_tx();

        let mut sch = Schema::new();
        sch.add_int_field(&"A".to_string()).unwrap();
        sch.add_string_field(&"B".to_string(), 9).unwrap();
        let layout = Arc::new(Mutex::new(
            Layout::new_from_schema(Arc::new(Mutex::new(sch))).unwrap(),
        ));

        {
            let binding = layout.lock().unwrap().schema().lock().unwrap().fields();
            let binding = binding.lock().unwrap();
            let fldnames = binding.iter();

            for fldname in fldnames {
                let offset = layout.lock().unwrap().offset(fldname).unwrap();
                if fldname == "A" {
                    assert_eq!(offset, 4)
                } else if fldname == "B" {
                    assert_eq!(offset, 8)
                } else {
                    panic!("unreachable!!")
                }
            }
        }

        let blk = tx.lock().unwrap().append("testfile".to_string()).unwrap();
        tx.lock().unwrap().pin(&blk.clone()).unwrap();
        let mut rp = RecordPage::new(tx.clone(), blk.clone(), layout).unwrap();
        rp.format().unwrap();

        println!("Filling the page with random records");
        let mut slot = rp.insert_after(-1).unwrap();
        let mut rng = rand::rng();

        while slot >= 0 {
            let n = rng.random_range(0..=50);
            rp.set_int(slot, "A".to_string(), n).unwrap();
            rp.set_string(slot, "B".to_string(), format!("rec{}", n))
                .unwrap();

            println!("Inserting into slot {}  [ {} , rec{} ]", slot, n, n);
            slot = rp.insert_after(slot).unwrap();
        }

        println!("Deleting these records, whose A-values are less than 25 ");

        slot = rp.next_after(-1).unwrap();
        while slot >= 0 {
            let a = rp.get_int(slot, "A".to_string()).unwrap();
            let b = rp.get_string(slot, "B".to_string()).unwrap();
            if a < 25 {
                println!("slot + {} : [ {} , {} ]", slot, a, b);
                rp.delete(slot).unwrap();
            }
            slot = rp.next_after(slot).unwrap();
        }

        println!("under 25 were deleted");

        println!("Here are the remaining records.");

        slot = rp.next_after(-1).unwrap();
        while slot >= 0 {
            let a = rp.get_int(slot, "A".to_string()).unwrap();
            let b = rp.get_string(slot, "B".to_string()).unwrap();
            assert!(a >= 25);
            println!("slot + {} : [ {} , {} ]", slot, a, b);
            slot = rp.next_after(slot).unwrap();
        }
        tx.lock().unwrap().unpin(&blk).unwrap();
        tx.lock().unwrap().commit().unwrap();
    }
}
