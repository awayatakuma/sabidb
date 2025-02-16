use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread, time,
};

use crate::file::block_id::BlockId;

use super::lock_table::{LockAbortException, LockTable};
use chrono::Utc;
use lazy_static::lazy_static;

const MAX_TIME: i64 = 10_000; // 10 seconds

lazy_static! {
    static ref LOCK_TABLE: Arc<Mutex<LockTable>> = Arc::new(Mutex::new(LockTable::new()));
}

#[derive(Debug, Clone)]
pub struct ConcurrencyManager {
    locks: Arc<Mutex<HashMap<BlockId, char>>>,
}

impl ConcurrencyManager {
    pub fn new() -> Self {
        ConcurrencyManager {
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn s_lock(&self, blk: &BlockId) -> Result<(), String> {
        if self
            .locks
            .lock()
            .map_err(|_| "failed to get lock")?
            .get(blk)
            .is_none()
        {
            self.s_lock_internal(blk).map_err(|e| e.to_string())?;

            self.locks
                .lock()
                .map_err(|_| "failed to get lock")?
                .insert(blk.clone(), 'S');
        }
        Ok(())
    }

    pub fn x_lock(&self, blk: &BlockId) -> Result<(), String> {
        if !self.has_x_lock(blk)? {
            self.s_lock(blk)?;
            self.x_lock_internal(blk).map_err(|e| e.to_string())?;
            self.locks
                .lock()
                .map_err(|_| "failed to get lock")?
                .insert(blk.clone(), 'X');
        }
        Ok(())
    }

    // This function corresponds to s_lock in LockTable in the original implementation
    fn s_lock_internal(&self, blk: &BlockId) -> Result<(), LockAbortException> {
        let timestamp = Utc::now().timestamp_millis();

        while !Self::waiting_too_long(timestamp) {
            if let Ok(lock_table) = LOCK_TABLE.lock() {
                if !lock_table.has_x_lock(blk).map_err(|_| LockAbortException)? {
                    let val = lock_table
                        .get_lock_val(blk)
                        .map_err(|_| LockAbortException)?;
                    lock_table
                        .locks
                        .lock()
                        .map_err(|_| LockAbortException)?
                        .insert(blk.clone(), val + 1);
                    return Ok(());
                }
            }

            thread::sleep(time::Duration::from_millis(100));
        }

        return Err(From::from(LockAbortException));
    }

    // This function corresponds to x_lock in LockTable in the original implementation
    fn x_lock_internal(&self, blk: &BlockId) -> Result<(), LockAbortException> {
        let timestamp = Utc::now().timestamp_millis();

        while !Self::waiting_too_long(timestamp) {
            if let Ok(lock_table) = LOCK_TABLE.lock() {
                if !lock_table
                    .has_other_s_locks(blk)
                    .map_err(|_| LockAbortException)?
                {
                    lock_table
                        .locks
                        .lock()
                        .map_err(|_| LockAbortException)?
                        .insert(blk.clone(), -1);
                    return Ok(());
                }
            }

            thread::sleep(time::Duration::from_millis(100));
        }

        return Err(From::from(LockAbortException));
    }

    pub fn release(&mut self) -> Result<(), String> {
        let mut locks = self.locks.lock().map_err(|_| "failed to get lock")?;

        // `unlock`操作を実行し、エラーがあれば返す
        for (blk, _) in locks.iter() {
            LOCK_TABLE
                .lock()
                .map_err(|_| "failed to get lock")?
                .unlock(blk)
                .map_err(|e| format!("failed to unlock block {}: {}", blk, e))?;
        }

        locks.clear();
        Ok(())
    }

    fn has_x_lock(&self, blk: &BlockId) -> Result<bool, String> {
        let locktype = self
            .locks
            .lock()
            .map_err(|_| "failed to get lock")?
            .get(blk)
            .cloned();
        if let Some(c) = locktype {
            Ok(c == 'X')
        } else {
            Ok(false)
        }
    }

    fn waiting_too_long(starttime: i64) -> bool {
        Utc::now().timestamp_millis() - starttime > MAX_TIME
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time};

    use tempfile::TempDir;

    use crate::{
        file::block_id::BlockId, server::simple_db::SimpleDB, tx::transaction::Transaction,
    };

    #[test]
    fn test_concurrency_manager() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(SimpleDB::new(temp_dir.path(), 400, 8));

        let db_a = db.clone();
        let db_b = db.clone();
        let db_c = db.clone();

        let handle_a = thread::spawn(move || {
            let mut tx_a = Transaction::new_from_managers(
                db_a.file_manager(),
                db_a.log_mgr(),
                db_a.buffer_manager(),
            )
            .unwrap();
            let blk1 = BlockId::new("testfile".to_string(), 1);
            let blk2 = BlockId::new("testfile".to_string(), 2);
            tx_a.pin(&blk1).unwrap();
            tx_a.pin(&blk2).unwrap();

            println!("Tx A: request slock 1");
            let acutual = tx_a.get_int(&blk1, 0).unwrap();
            println!("Tx A: receive slock 1");
            assert_eq!(acutual, 0);

            thread::sleep(time::Duration::from_millis(1_000.try_into().unwrap()));

            println!("Tx A: request slock 2");
            let acutual = tx_a.get_int(&blk2, 0).unwrap();
            println!("Tx A: receive slock 2");
            assert_eq!(acutual, 1);

            tx_a.commit().unwrap();
            println!("Tx A: commit");
        });

        let handle_b = thread::spawn(move || {
            let mut tx_b = Transaction::new_from_managers(
                db_b.file_manager(),
                db_b.log_mgr(),
                db_b.buffer_manager(),
            )
            .unwrap();
            let blk1 = BlockId::new("testfile".to_string(), 1);
            let blk2 = BlockId::new("testfile".to_string(), 2);
            tx_b.pin(&blk1).unwrap();
            tx_b.pin(&blk2).unwrap();

            println!("Tx B: request xlock 2");
            tx_b.set_int(&blk2, 0, 1, false).unwrap();
            println!("Tx B: recieve xlock 2");

            thread::sleep(time::Duration::from_millis(1_000.try_into().unwrap()));

            println!("Tx B: request slock 1");
            let actual = tx_b.get_int(&blk1, 0).unwrap();
            println!("Tx B: recieve slock 1");
            assert_eq!(actual, 0);

            tx_b.commit().unwrap();
            println!("Tx B: commit");
        });

        let handle_c = thread::spawn(move || {
            let mut tx_c = Transaction::new_from_managers(
                db_c.file_manager(),
                db_c.log_mgr(),
                db_c.buffer_manager(),
            )
            .unwrap();
            let blk1 = BlockId::new("testfile".to_string(), 1);
            let blk2 = BlockId::new("testfile".to_string(), 2);
            tx_c.pin(&blk1).unwrap();
            tx_c.pin(&blk2).unwrap();

            thread::sleep(time::Duration::from_millis(500.try_into().unwrap()));

            println!("Tx C: request xlock 1");
            tx_c.set_int(&blk1, 0, 2, false).unwrap();
            println!("Tx C: recieve xlock 1");

            thread::sleep(time::Duration::from_millis(1_000.try_into().unwrap()));

            println!("Tx C: request slock 2");
            let actual = tx_c.get_int(&blk2, 0).unwrap();
            println!("Tx C: recieve slock 2");
            assert_eq!(actual, 1);

            tx_c.commit().unwrap();

            println!("Tx C: commit");
        });

        handle_a.join().unwrap();
        handle_b.join().unwrap();
        handle_c.join().unwrap();
    }
}
