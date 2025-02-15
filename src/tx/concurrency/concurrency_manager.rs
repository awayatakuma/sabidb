use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::file::block_id::BlockId;

use super::lock_table::LockTable;
use lazy_static::lazy_static;

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
        if !self
            .locks
            .lock()
            .map_err(|_| "failed to get lock")?
            .contains_key(blk)
        {
            LOCK_TABLE
                .lock()
                .map_err(|_| "failed to get lock")?
                .s_lock(blk)
                .map_err(|e| e.to_string())?;
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
            LOCK_TABLE
                .lock()
                .map_err(|_| "failed to get lock")?
                .x_lock(blk)
                .map_err(|e| e.to_string())?;
            self.locks
                .lock()
                .map_err(|_| "failed to get lock")?
                .insert(blk.clone(), 'X');
        }
        Ok(())
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
}
