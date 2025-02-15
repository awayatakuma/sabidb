use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::{buffer::Buffer, buffer_manager::BufferManager},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
};

use super::{
    checkpoint_record::CheckpointRecord,
    commit_record::CommitRecord,
    log_record::{self},
    rollback_record::RollbackRecord,
    set_int_record::SetIntRecord,
    set_string_record::SetStringRecord,
    start_record::StartRecord,
};

#[derive(Debug, Clone)]
pub struct RecoveryManager {
    lm: Arc<Mutex<LogManager>>,
    bm: Arc<Mutex<BufferManager>>,
    tx: Arc<Mutex<Transaction>>,
    txnum: i32,
}

impl RecoveryManager {
    pub fn new_from_managers(
        tx: Arc<Mutex<Transaction>>,
        txnum: i32,
        lm: Arc<Mutex<LogManager>>,
        bm: Arc<Mutex<BufferManager>>,
    ) -> Result<Self, String> {
        StartRecord::write_to_log(lm.clone(), txnum)?;
        Ok(RecoveryManager { tx, txnum, lm, bm })
    }

    pub fn commit(&mut self) -> Result<(), String> {
        self.bm
            .lock()
            .map_err(|_| "failed to get lock")?
            .flush_all(self.txnum)?;
        let lsn = CommitRecord::write_to_log(self.lm.clone(), self.txnum)?;
        self.lm
            .lock()
            .map_err(|_| "failed to get lock")?
            .flush(lsn)?;

        Ok(())
    }

    pub fn rollback(&self) -> Result<(), String> {
        self.do_rollback()?;
        self.bm
            .lock()
            .map_err(|_| "failed to get lock")?
            .flush_all(self.txnum)?;

        let lsn = RollbackRecord::write_to_log(self.lm.clone(), self.txnum)?;
        self.lm
            .lock()
            .map_err(|_| "failed to get lock")?
            .flush(lsn)?;

        Ok(())
    }

    pub fn recover(&self) -> Result<(), String> {
        self.do_recover()?;
        self.bm
            .lock()
            .map_err(|_| "failed to get lock")?
            .flush_all(self.txnum)?;
        let lsn = CheckpointRecord::write_to_log(self.lm.clone())?;
        self.lm
            .lock()
            .map_err(|_| "failed to get lock")?
            .flush(lsn)?;

        Ok(())
    }

    pub fn set_int(&self, buff: &mut Buffer, offset: usize, _newval: i32) -> Result<i32, String> {
        let oldval = buff.contents().get_int(offset)?;
        let blk = buff.block().ok_or("cannot get BlockId")?;
        SetIntRecord::write_to_log(self.lm.clone(), self.txnum, &blk, offset as i32, oldval)
    }

    pub fn set_string(
        &self,
        buff: &mut Buffer,
        offset: usize,
        _newval: String,
    ) -> Result<i32, String> {
        let oldval = buff.contents().get_string(offset)?;
        let blk = buff.block().ok_or("cannot get BlockId")?;
        SetStringRecord::write_to_log(self.lm.clone(), self.txnum, &blk, offset as i32, oldval)
    }

    fn do_rollback(&self) -> Result<(), String> {
        let mut iter = self
            .lm
            .lock()
            .map_err(|_| "failed to get lock")?
            .iterator()?;
        while let Some(elem) = iter.next() {
            let bytes = elem?;
            let rec = log_record::create_log_record(bytes)?;
            if rec.tx_number() == self.txnum {
                if rec.op() == log_record::START {
                    return Ok(());
                }
                rec.undo(self.tx.clone())?;
            }
        }
        Ok(())
    }

    fn do_recover(&self) -> Result<(), String> {
        let mut finished_txs = HashSet::new();
        let mut iter = self
            .lm
            .lock()
            .map_err(|_| "failed to get lock")?
            .iterator()?;

        while let Some(elem) = iter.next() {
            let bytes = elem?;
            let rec = log_record::create_log_record(bytes)?;
            if rec.op() == log_record::CHECKPOINT {
                return Ok(());
            }
            if rec.op() == log_record::COMMIT || rec.op() == log_record::ROLLBACK {
                finished_txs.insert(rec.tx_number());
            } else if !finished_txs.contains(&rec.tx_number()) {
                rec.undo(self.tx.clone())?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::{
        constants::INTEGER_BYTES,
        file::{block_id::BlockId, page::Page},
        server::simple_db::SimpleDB,
    };

    #[test]
    fn test_recovery_manager() {
        let db = SimpleDB::new(&Path::new("/tmp/buffertest"), 400, 8);
        let fm = db.file_manager();
        let bm = db.buffer_manager();
        let blk0 = BlockId::new("testfile".to_string(), 0);
        let blk1 = BlockId::new("testfile".to_string(), 1);

        if fm.lock().unwrap().len(&"testfile".to_string()).unwrap() == 0 {
            // initialize
            let tx1 = db.new_tx();
            let tx2 = db.new_tx();
            tx1.lock().unwrap().pin(&blk0).unwrap();
            tx2.lock().unwrap().pin(&blk1).unwrap();
            let mut pos = 0;
            for _ in 0..6 {
                tx1.lock()
                    .unwrap()
                    .set_int(&blk0, pos, pos as i32, false)
                    .unwrap();
                tx2.lock()
                    .unwrap()
                    .set_int(&blk1, pos, pos as i32, false)
                    .unwrap();
                pos += INTEGER_BYTES;
            }
            tx1.lock()
                .unwrap()
                .set_string(&blk0, 30, "abc".to_string(), false)
                .unwrap();
            tx2.lock()
                .unwrap()
                .set_string(&blk1, 30, "def".to_string(), false)
                .unwrap();
            tx1.lock().unwrap().commit().unwrap();
            tx2.lock().unwrap().commit().unwrap();

            // assert
            println!("After initialization");
            let mut p0 = Page::new_from_blocksize(fm.lock().unwrap().block_size() as usize);
            let mut p1 = Page::new_from_blocksize(fm.lock().unwrap().block_size() as usize);
            fm.lock().unwrap().read(&blk0, &mut p0).unwrap();
            fm.lock().unwrap().read(&blk1, &mut p1).unwrap();
            let mut pos = 0;
            for _ in 0..6 {
                assert_eq!(p0.get_int(pos).unwrap(), pos as i32);
                assert_eq!(p1.get_int(pos).unwrap(), pos as i32);
                pos += INTEGER_BYTES;
            }
            assert_eq!(p0.get_string(30).unwrap(), "abc");
            assert_eq!(p1.get_string(30).unwrap(), "def");

            // modify
            let tx3 = db.new_tx();
            let tx4 = db.new_tx();
            tx3.lock().unwrap().pin(&blk0).unwrap();
            tx4.lock().unwrap().pin(&blk1).unwrap();
            let mut pos = 0;
            for _ in 0..6 {
                tx3.lock()
                    .unwrap()
                    .set_int(&blk0, pos, pos as i32 + 100, true)
                    .unwrap();
                tx4.lock()
                    .unwrap()
                    .set_int(&blk1, pos, pos as i32 + 100, true)
                    .unwrap();
                pos += INTEGER_BYTES;
            }
            tx3.lock()
                .unwrap()
                .set_string(&blk0, 30, "uvw".to_string(), true)
                .unwrap();
            tx4.lock()
                .unwrap()
                .set_string(&blk1, 30, "xyz".to_string(), true)
                .unwrap();
            bm.lock().unwrap().flush_all(3).unwrap();
            bm.lock().unwrap().flush_all(4).unwrap();

            // assert
            println!("After modification");
            let mut p0 = Page::new_from_blocksize(fm.lock().unwrap().block_size() as usize);
            let mut p1 = Page::new_from_blocksize(fm.lock().unwrap().block_size() as usize);
            fm.lock().unwrap().read(&blk0, &mut p0).unwrap();
            fm.lock().unwrap().read(&blk1, &mut p1).unwrap();
            let mut pos = 0;
            for _ in 0..6 {
                assert_eq!(p0.get_int(pos).unwrap(), pos as i32 + 100);
                assert_eq!(p1.get_int(pos).unwrap(), pos as i32 + 100);
                pos += INTEGER_BYTES;
            }
            assert_eq!(p0.get_string(30).unwrap(), "uvw");
            assert_eq!(p1.get_string(30).unwrap(), "xyz");

            // rollback
            tx3.lock().unwrap().rollback().unwrap();

            // assert
            println!("After rollback");
            let mut p0 = Page::new_from_blocksize(fm.lock().unwrap().block_size() as usize);
            let mut p1 = Page::new_from_blocksize(fm.lock().unwrap().block_size() as usize);
            fm.lock().unwrap().read(&blk0, &mut p0).unwrap();
            fm.lock().unwrap().read(&blk1, &mut p1).unwrap();
            let mut pos = 0;
            for _ in 0..6 {
                assert_eq!(p0.get_int(pos).unwrap(), pos as i32);
                assert_eq!(p1.get_int(pos).unwrap(), pos as i32 + 100);
                pos += INTEGER_BYTES;
            }
            assert_eq!(p0.get_string(30).unwrap(), "abc");
            assert_eq!(p1.get_string(30).unwrap(), "xyz");
        } else {
            // recovery
            // tx3 is already rollbacked but tx4 is in neither states of commit nor rollback
            let tx = db.new_tx();
            tx.lock().unwrap().recover().unwrap();

            // assert
            println!("After recovery");
            let mut p0 = Page::new_from_blocksize(fm.lock().unwrap().block_size() as usize);
            let mut p1 = Page::new_from_blocksize(fm.lock().unwrap().block_size() as usize);
            fm.lock().unwrap().read(&blk0, &mut p0).unwrap();
            fm.lock().unwrap().read(&blk1, &mut p1).unwrap();
            let mut pos = 0;
            for _ in 0..6 {
                assert_eq!(p0.get_int(pos).unwrap(), pos as i32);
                assert_eq!(p1.get_int(pos).unwrap(), pos as i32);
                println!("{}", p0.get_int(pos).unwrap());
                println!("{}", p1.get_int(pos).unwrap());
                pos += INTEGER_BYTES;
            }
            assert_eq!(p0.get_string(30).unwrap(), "abc");
            assert_eq!(p1.get_string(30).unwrap(), "def");
        }
    }
}
