use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffer_manager::BufferManager,
    log::log_manager::LogManager,
};

use super::{
    checkpoint_record::CheckpointRecord, commit_record::CommitRecord,
    log_record::{self, create_log_record},
    rollback_record::RollbackRecord, set_int_record::SetIntRecord,
    set_string_record::SetStringRecord, start_record::StartRecord,
};

#[derive(Debug)]
pub struct RecoveryManager {
    tx: Arc<Mutex<crate::tx::transaction::Transaction>>,
    txnum: i32,
    lm: Arc<Mutex<LogManager>>,
    bm: Arc<Mutex<BufferManager>>,
}

impl RecoveryManager {
    pub fn new_from_managers(
        tx: Arc<Mutex<crate::tx::transaction::Transaction>>,
        txnum: i32,
        lm: Arc<Mutex<LogManager>>,
        bm: Arc<Mutex<BufferManager>>,
    ) -> Result<Self, String> {
        StartRecord::write_to_log(lm.clone(), txnum)?;
        Ok(Self { tx, txnum, lm, bm })
    }

    pub fn commit(&mut self) -> Result<(), String> {
        self.bm.lock().unwrap().flush_all(self.txnum)?;
        let lsn = CommitRecord::write_to_log(self.lm.clone(), self.txnum)?;
        self.lm.lock().unwrap().flush(lsn)?;
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), String> {
        self.do_rollback()?;
        self.bm.lock().unwrap().flush_all(self.txnum)?;
        let lsn = RollbackRecord::write_to_log(self.lm.clone(), self.txnum)?;
        self.lm.lock().unwrap().flush(lsn)?;
        Ok(())
    }

    pub fn recover(&mut self) -> Result<(), String> {
        self.do_recover()?;
        self.bm.lock().unwrap().flush_all(self.txnum)?;
        let lsn = CheckpointRecord::write_to_log(self.lm.clone())?;
        self.lm.lock().unwrap().flush(lsn)?;
        Ok(())
    }

    pub fn set_int(&mut self, buff: Arc<Mutex<crate::buffer::buffer::Buffer>>, offset: i32) -> Result<i32, String> {
        let oldval = buff.lock().unwrap().contents().get_int(offset as usize)?;
        let blk = buff.lock().unwrap().block().unwrap();
        let lsn = SetIntRecord::write_to_log(self.lm.clone(), self.txnum, &blk, offset, oldval)?;
        Ok(lsn)
    }

    pub fn set_string(&mut self, buff: Arc<Mutex<crate::buffer::buffer::Buffer>>, offset: i32) -> Result<i32, String> {
        let oldval = buff.lock().unwrap().contents().get_string(offset as usize)?;
        let blk = buff.lock().unwrap().block().unwrap();
        let lsn = SetStringRecord::write_to_log(self.lm.clone(), self.txnum, &blk, offset, oldval)?;
        Ok(lsn)
    }

    fn do_rollback(&mut self) -> Result<(), String> {
        let mut iter = self.lm.lock().unwrap().iterator()?;
        while let Some(bytes_res) = iter.next() {
            let bytes = bytes_res?;
            let rec = create_log_record(bytes)?;
            if rec.tx_number() == self.txnum {
                if rec.op() == log_record::START {
                    return Ok(());
                }
                rec.undo(self.tx.clone())?;
            }
        }
        Ok(())
    }

    fn do_recover(&mut self) -> Result<(), String> {
        let mut finished_txs = Vec::new();
        let mut iter = self.lm.lock().unwrap().iterator()?;
        while let Some(bytes_res) = iter.next() {
            let bytes = bytes_res?;
            let rec = create_log_record(bytes)?;
            if rec.op() == log_record::CHECKPOINT {
                return Ok(());
            }
            if rec.op() == log_record::COMMIT || rec.op() == log_record::ROLLBACK {
                finished_txs.push(rec.tx_number());
            } else if !finished_txs.contains(&rec.tx_number()) {
                rec.undo(self.tx.clone())?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::{
        constants::INTEGER_BYTES,
        file::{block_id::BlockId, file_manager::FileManager, page::Page},
        server::simple_db::SimpleDB,
    };

    #[test]
    fn test_recovery_manager_1() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new_with_sizes(temp_dir.path(), 400, 8);
        let fm = db.file_manager();
        let bm = db.buffer_manager();
        let blk0 = BlockId::new("testfile".to_string(), 0);
        let blk1 = BlockId::new("testfile".to_string(), 1);

        // initialize
        let tx1 = db.new_tx();
        let tx2 = db.new_tx();
        tx1.lock().unwrap().pin(&blk0).unwrap();
        tx2.lock().unwrap().pin(&blk1).unwrap();
        let mut pos = 0;
        for _ in 0..6 {
            tx1.lock()
                .unwrap()
                .set_int(&blk0, pos, pos as i32, true)
                .unwrap();
            tx2.lock()
                .unwrap()
                .set_int(&blk1, pos, pos as i32, true)
                .unwrap();
            pos += INTEGER_BYTES as usize;
        }
        tx1.lock()
            .unwrap()
            .set_string(&blk0, 30, "abc".to_string(), true)
            .unwrap();
        tx2.lock()
            .unwrap()
            .set_string(&blk1, 30, "def".to_string(), true)
            .unwrap();

        tx1.lock().unwrap().commit().unwrap();
        tx2.lock().unwrap().commit().unwrap();

        print_values(&fm, &blk0, &blk1, "After initialization");

        // modification
        let tx3 = db.new_tx();
        let tx4 = db.new_tx();
        tx3.lock().unwrap().pin(&blk0).unwrap();
        tx4.lock().unwrap().pin(&blk1).unwrap();
        pos = 0;
        for _ in 0..6 {
            tx3.lock()
                .unwrap()
                .set_int(&blk0, pos, pos as i32 + 100, true)
                .unwrap();
            tx4.lock()
                .unwrap()
                .set_int(&blk1, pos, pos as i32 + 100, true)
                .unwrap();
            pos += INTEGER_BYTES as usize;
        }
        tx3.lock()
            .unwrap()
            .set_string(&blk0, 30, "uvw".to_string(), true)
            .unwrap();
        tx4.lock()
            .unwrap()
            .set_string(&blk1, 30, "xyz".to_string(), true)
            .unwrap();

        let tx3_num = tx3.lock().unwrap().tx_num();
        let tx4_num = tx4.lock().unwrap().tx_num();
        bm.lock().unwrap().flush_all(tx3_num).unwrap();
        bm.lock().unwrap().flush_all(tx4_num).unwrap();

        print_values(&fm, &blk0, &blk1, "After modification");

        tx3.lock().unwrap().rollback().unwrap();
        print_values(&fm, &blk0, &blk1, "After rollback");

        let mut p0 = Page::new_from_blocksize(400);
        let mut p1 = Page::new_from_blocksize(400);
        fm.read(&blk0, &mut p0).unwrap();
        fm.read(&blk1, &mut p1).unwrap();
        pos = 0;
        for _ in 0..6 {
            assert_eq!(p0.get_int(pos).unwrap(), pos as i32);
            assert_eq!(p1.get_int(pos).unwrap(), pos as i32 + 100);
            pos += INTEGER_BYTES as usize;
        }
        assert_eq!(p0.get_string(30).unwrap(), "abc");
        assert_eq!(p1.get_string(30).unwrap(), "xyz");
    }

    #[test]
    fn test_recovery_manager_2() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new_with_sizes(temp_dir.path(), 400, 8);

        let blk0 = BlockId::new("testfile".to_string(), 0);
        let blk1 = BlockId::new("testfile".to_string(), 1);

        // initialize
        let tx1 = db.new_tx();
        let tx2 = db.new_tx();
        tx1.lock().unwrap().pin(&blk0).unwrap();
        tx2.lock().unwrap().pin(&blk1).unwrap();
        let mut pos = 0;
        for _ in 0..6 {
            tx1.lock()
                .unwrap()
                .set_int(&blk0, pos, pos as i32, true)
                .unwrap();
            tx2.lock()
                .unwrap()
                .set_int(&blk1, pos, pos as i32, true)
                .unwrap();
            pos += INTEGER_BYTES as usize;
        }
        tx1.lock()
            .unwrap()
            .set_string(&blk0, 30, "abc".to_string(), true)
            .unwrap();
        tx2.lock()
            .unwrap()
            .set_string(&blk1, 30, "def".to_string(), true)
            .unwrap();

        tx1.lock().unwrap().commit().unwrap();
        tx2.lock().unwrap().commit().unwrap();

        // recover
        let tx3 = db.new_tx();
        tx3.lock().unwrap().recover().unwrap();

        let fm = db.file_manager();
        let mut p0 = Page::new_from_blocksize(400);
        let mut p1 = Page::new_from_blocksize(400);
        fm.read(&blk0, &mut p0).unwrap();
        fm.read(&blk1, &mut p1).unwrap();
        let mut pos = 0;
        for _ in 0..6 {
            assert_eq!(p0.get_int(pos).unwrap(), pos as i32);
            assert_eq!(p1.get_int(pos).unwrap(), pos as i32);
            pos += INTEGER_BYTES as usize;
        }
        assert_eq!(p0.get_string(30).unwrap(), "abc");
        assert_eq!(p1.get_string(30).unwrap(), "def");
    }

    fn print_values(
        fm: &Arc<FileManager>,
        blk0: &BlockId,
        blk1: &BlockId,
        msg: &str,
    ) {
        println!("{}", msg);
        let mut p0 = Page::new_from_blocksize(400);
        let mut p1 = Page::new_from_blocksize(400);
        fm.read(blk0, &mut p0).unwrap();
        fm.read(blk1, &mut p1).unwrap();
        let mut pos = 0;
        for _ in 0..6 {
            print!("{} ", p0.get_int(pos).unwrap());
            print!("{} ", p1.get_int(pos).unwrap());
            pos += INTEGER_BYTES as usize;
        }
        println!("{}", p0.get_string(30).unwrap());
        println!("{}", p1.get_string(30).unwrap());
        println!();
    }
}
