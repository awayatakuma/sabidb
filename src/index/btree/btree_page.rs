use std::sync::{Arc, Mutex};

use crate::{
    constants::INTEGER_BYTES,
    file::block_id::BlockId,
    query::constant::Constant,
    record::{
        layout::Layout,
        rid::RID,
        schema::field_type::{INTEGER, VARCHAR},
    },
    tx::transaction::Transaction,
};

pub struct BTPage {
    tx: Arc<Mutex<Transaction>>,
    currentblk: Option<BlockId>,
    layout: Layout,
}

impl BTPage {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        currentblk: BlockId,
        layout: Layout,
    ) -> Result<Self, String> {
        tx.lock()
            .map_err(|_| "failed to get lock")?
            .pin(&currentblk)?;
        Ok(BTPage {
            tx,
            currentblk: Some(currentblk),
            layout,
        })
    }

    pub fn find_slot_before(&self, search_key: &Constant) -> Result<i32, String> {
        let mut slot = 0;
        while slot < self.get_num_recs()?
            && self.get_data_val(slot)?.partial_cmp(search_key) == Some(std::cmp::Ordering::Less)
        {
            slot += 1;
        }

        Ok(slot - 1)
    }

    pub fn close(&mut self) -> Result<(), String> {
        if let Some(blk) = &self.currentblk {
            self.tx
                .lock()
                .map_err(|_| "failed to get lock")?
                .unpin(blk)?;
        }

        self.currentblk = None;
        Ok(())
    }

    pub fn is_full(&self) -> Result<bool, String> {
        Ok(self.slotpos(self.get_num_recs()? + 1)
            > self
                .tx
                .lock()
                .map_err(|_| "failed to get lock")?
                .block_size())
    }

    pub fn split(&self, splitpos: i32, flag: i32) -> Result<BlockId, String> {
        let newblk = self.append_new(flag)?;
        let mut newpage = BTPage::new(self.tx.clone(), newblk.clone(), self.layout.clone())?;
        self.transfer_recs(splitpos, &mut newpage)?;
        newpage.set_flag(flag)?;
        newpage.close()?;
        Ok(newblk)
    }

    pub fn get_data_val(&self, slot: i32) -> Result<Constant, String> {
        self.get_val(slot, "dataval".to_string())
    }

    pub fn get_flag(&self) -> Result<i32, String> {
        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_int(&self.currentblk.clone().unwrap(), 0)
    }

    pub fn set_flag(&self, val: i32) -> Result<(), String> {
        self.tx.lock().map_err(|_| "failed to get lock")?.set_int(
            &self.currentblk.clone().unwrap(),
            0,
            val,
            true,
        )
    }

    pub fn append_new(&self, flag: i32) -> Result<BlockId, String> {
        let blk = self
            .tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .append(self.currentblk.clone().unwrap().file_name())?;
        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .pin(&blk)?;
        self.format(&blk, flag)?;
        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .unpin(&blk)?;
        Ok(blk)
    }

    pub fn format(&self, blk: &BlockId, flag: i32) -> Result<(), String> {
        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .set_int(blk, 0, flag, false)?;
        self.tx.lock().map_err(|_| "failed to get lock")?.set_int(
            blk,
            INTEGER_BYTES as usize,
            0,
            false,
        )?;
        let recsize = self.layout.slot_size();
        let mut pos = 2 * INTEGER_BYTES;
        while pos + recsize
            <= self
                .tx
                .lock()
                .map_err(|_| "failed to get lock")?
                .block_size()?
        {
            self.make_default_record(blk, pos)?;
            pos += recsize;
        }
        Ok(())
    }

    fn make_default_record(&self, blk: &BlockId, pos: i32) -> Result<(), String> {
        let binding = self.layout.schema().fields();
        let binding = binding.lock().map_err(|_| "failed to get lock")?;
        let flds = binding.iter();
        for fldname in flds {
            let offset = self.layout.offset(fldname)?;
            if self.layout.schema().field_type(fldname)? == INTEGER {
                self.tx.lock().map_err(|_| "failed to get lock")?.set_int(
                    blk,
                    pos as usize + offset,
                    0,
                    false,
                )?;
            } else if self.layout.schema().field_type(fldname)? == VARCHAR {
                self.tx
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .set_string(blk, pos as usize + offset, "".to_string(), false)?;
            } else {
                panic!("Unreachable!!")
            }
        }

        Ok(())
    }

    pub fn get_child_num(&self, slot: i32) -> Result<i32, String> {
        self.get_int(slot, "block".to_string())
    }

    pub fn insert_dir(&self, slot: i32, val: Constant, blknum: i32) -> Result<(), String> {
        self.insert(slot)?;
        self.set_val(slot, "dataval".to_string(), val)?;
        self.set_int(slot, "block".to_string(), blknum)?;
        Ok(())
    }

    pub fn get_data_rid(&self, slot: i32) -> Result<RID, String> {
        Ok(RID::new(
            self.get_int(slot, "block".to_string())?,
            self.get_int(slot, "id".to_string())?,
        ))
    }

    pub fn insert_leaf(&self, slot: i32, val: Constant, rid: &RID) -> Result<(), String> {
        self.insert(slot)?;
        self.set_val(slot, "dataval".to_string(), val)?;
        self.set_int(slot, "block".to_string(), rid.block_number())?;
        self.set_int(slot, "id".to_string(), rid.slot())?;

        Ok(())
    }

    pub fn delete(&self, slot: i32) -> Result<(), String> {
        let mut i = slot + 1;
        while i < self.get_num_recs()? {
            self.copy_record(i, i - 1)?;
            i += 1;
        }
        self.set_num_recs(self.get_num_recs()? - 1)?;
        Ok(())
    }

    pub fn get_num_recs(&self) -> Result<i32, String> {
        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_int(self.currentblk.as_ref().unwrap(), INTEGER_BYTES as usize)
    }

    fn get_int(&self, slot: i32, fldname: String) -> Result<i32, String> {
        let pos = self.fldpos(slot, fldname)?;
        return self
            .tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_int(self.currentblk.as_ref().unwrap(), pos as usize);
    }

    fn get_string(&self, slot: i32, fldname: String) -> Result<String, String> {
        let pos = self.fldpos(slot, fldname)?;
        return self
            .tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_string(self.currentblk.as_ref().unwrap(), pos as usize);
    }

    fn get_val(&self, slot: i32, fldname: String) -> Result<Constant, String> {
        let fldtype = self.layout.schema().field_type(&fldname)?;
        if fldtype == INTEGER {
            Ok(Constant::new_from_i32(self.get_int(slot, fldname)?))
        } else if fldtype == VARCHAR {
            Ok(Constant::new_from_string(self.get_string(slot, fldname)?))
        } else {
            panic!("Unreachable!!")
        }
    }

    fn set_int(&self, slot: i32, fldname: String, val: i32) -> Result<(), String> {
        let pos = self.fldpos(slot, fldname)?;
        self.tx.lock().map_err(|_| "failed to get lock")?.set_int(
            &self.currentblk.clone().unwrap(),
            pos as usize,
            val,
            true,
        )
    }

    fn set_string(&self, slot: i32, fldname: String, val: String) -> Result<(), String> {
        let pos = self.fldpos(slot, fldname)?;
        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .set_string(&self.currentblk.clone().unwrap(), pos as usize, val, true)
    }

    fn set_val(&self, slot: i32, fldname: String, val: Constant) -> Result<(), String> {
        let fldtype = self.layout.schema().field_type(&fldname)?;
        if fldtype == INTEGER {
            self.set_int(slot, fldname, val.as_int().unwrap())?;
        } else if fldtype == VARCHAR {
            self.set_string(slot, fldname, val.as_string().unwrap())?;
        } else {
            panic!("Unreachable")
        }

        Ok(())
    }

    fn set_num_recs(&self, n: i32) -> Result<(), String> {
        self.tx.lock().map_err(|_| "failed to get lock")?.set_int(
            &self.currentblk.clone().unwrap(),
            INTEGER_BYTES as usize,
            n,
            true,
        )
    }

    fn insert(&self, slot: i32) -> Result<(), String> {
        let mut i = self.get_num_recs()?;
        while i > slot {
            self.copy_record(i - 1, i)?;
            i -= 1;
        }
        self.set_num_recs(self.get_num_recs()? + 1)?;
        Ok(())
    }

    fn copy_record(&self, from: i32, to: i32) -> Result<(), String> {
        let fields: Vec<String> = self
            .layout
            .schema()
            .fields()
            .lock()
            .map_err(|_| "failed to get lock")?
            .clone();
        for fldname in fields {
            let val = self.get_val(from, fldname.clone())?;

            self.set_val(to, fldname, val)?;
        }
        Ok(())
    }

    fn transfer_recs(&self, slot: i32, dest: &mut BTPage) -> Result<(), String> {
        let mut destslot = 0;
        let fields: Vec<String> = self
            .layout
            .schema()
            .fields()
            .lock()
            .map_err(|_| "failed to get lock")?
            .clone();
        while slot < self.get_num_recs()? {
            dest.insert(destslot)?;
            for fldname in &fields {
                dest.set_val(
                    destslot,
                    fldname.clone(),
                    self.get_val(slot, fldname.clone())?,
                )?;
            }
            self.delete(slot)?;
            destslot += 1;
        }

        Ok(())
    }

    fn fldpos(&self, slot: i32, fldname: String) -> Result<i32, String> {
        let offset = self.layout.offset(&fldname)?;
        let ret = self.slotpos(slot)? + offset as i32;
        Ok(ret)
    }

    fn slotpos(&self, slot: i32) -> Result<i32, String> {
        let slotsize = self.layout.slot_size();
        Ok(INTEGER_BYTES + INTEGER_BYTES + (slot * slotsize))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread, time::Duration};

    use tempfile::TempDir;

    use crate::{
        plan::planner::Planner, server::simple_db::SimpleDB, tx::transaction::Transaction,
    };
    use std::sync::{Arc, Mutex};

    /// Runs `body` on its own thread so a deadlock in the split path fails the
    /// test instead of hanging the whole suite.
    fn within(secs: u64, body: impl FnOnce() + Send + 'static) {
        let (done, finished) = mpsc::channel();
        thread::spawn(move || {
            body();
            let _ = done.send(());
        });
        match finished.recv_timeout(Duration::from_secs(secs)) {
            Ok(()) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => panic!("still running after {secs}s"),
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("the body panicked"),
        }
    }

    fn keys_matching(planner: &mut Planner, tx: Arc<Mutex<Transaction>>, key: i32) -> Vec<String> {
        let qry = format!("select a, b from T where a = {}", key);
        let plan = planner.create_query_planner(&qry, tx).unwrap();
        let scan = plan.lock().unwrap().open().unwrap();
        let mut found = Vec::new();
        while scan.lock().unwrap().next().unwrap() {
            found.push(scan.lock().unwrap().get_string(&"b".to_string()).unwrap());
        }
        scan.lock().unwrap().close().unwrap();
        found.sort();
        found
    }

    /// Enough rows that the leaf page cannot hold them all.
    const ROWS: i32 = 400;

    #[test]
    fn split_keeps_every_key_reachable() {
        within(60, || {
            let temp_dir = TempDir::new().unwrap();
            let db = SimpleDB::new_with_refined_planners(temp_dir.path());
            let tx = db.new_tx();
            let mut planner = db.planner.clone().unwrap();

            planner
                .execute_update("create table T(a int, b varchar(9))", tx.clone())
                .unwrap();
            planner
                .execute_update("create index aidx on T(a)", tx.clone())
                .unwrap();
            for i in 0..ROWS {
                let cmd = format!("insert into T(a, b) values ({}, 'v{}')", i, i);
                planner.execute_update(&cmd, tx.clone()).unwrap();
            }

            // Without a split the whole index would still be one block.
            let leaf_blocks = tx.lock().unwrap().size("aidxleaf".to_string()).unwrap();
            assert!(leaf_blocks > 1, "no split happened: {} block", leaf_blocks);

            for key in [0, 1, ROWS / 2, ROWS - 1] {
                assert_eq!(
                    keys_matching(&mut planner, tx.clone(), key),
                    vec![format!("v{}", key)],
                    "key {} lost after splitting",
                    key
                );
            }
            assert!(keys_matching(&mut planner, tx.clone(), ROWS).is_empty());
            tx.lock().unwrap().commit().unwrap();
        });
    }

    #[test]
    fn split_survives_reopening() {
        within(60, || {
            let temp_dir = TempDir::new().unwrap();
            {
                let db = SimpleDB::new_with_refined_planners(temp_dir.path());
                let tx = db.new_tx();
                let mut planner = db.planner.clone().unwrap();
                planner
                    .execute_update("create table T(a int, b varchar(9))", tx.clone())
                    .unwrap();
                planner
                    .execute_update("create index aidx on T(a)", tx.clone())
                    .unwrap();
                for i in 0..ROWS {
                    let cmd = format!("insert into T(a, b) values ({}, 'v{}')", i, i);
                    planner.execute_update(&cmd, tx.clone()).unwrap();
                }
                tx.lock().unwrap().commit().unwrap();
            }

            let db = SimpleDB::new_with_refined_planners(temp_dir.path());
            let tx = db.new_tx();
            let mut planner = db.planner.clone().unwrap();
            assert_eq!(
                keys_matching(&mut planner, tx.clone(), ROWS / 2),
                vec![format!("v{}", ROWS / 2)]
            );
            tx.lock().unwrap().commit().unwrap();
        });
    }

    #[test]
    fn split_keeps_duplicate_keys_together() {
        within(60, || {
            let temp_dir = TempDir::new().unwrap();
            let db = SimpleDB::new_with_refined_planners(temp_dir.path());
            let tx = db.new_tx();
            let mut planner = db.planner.clone().unwrap();

            planner
                .execute_update("create table T(a int, b varchar(9))", tx.clone())
                .unwrap();
            planner
                .execute_update("create index aidx on T(a)", tx.clone())
                .unwrap();
            // One key repeated often enough to span a split, with other keys
            // around it.
            for i in 0..ROWS {
                let key = if i % 2 == 0 { 42 } else { i };
                let cmd = format!("insert into T(a, b) values ({}, 'v{}')", key, i);
                planner.execute_update(&cmd, tx.clone()).unwrap();
            }

            let mut expected: Vec<String> = (0..ROWS)
                .filter(|i| i % 2 == 0)
                .map(|i| format!("v{}", i))
                .collect();
            expected.sort();
            assert_eq!(keys_matching(&mut planner, tx.clone(), 42), expected);
            tx.lock().unwrap().commit().unwrap();
        });
    }
}
