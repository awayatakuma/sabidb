use chrono::Utc;
use core::{fmt, time};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
};

use crate::file::block_id::BlockId;

const MAX_TIME: i64 = 10_000; // 10 seconds

pub struct LockTable {
    locks: Arc<Mutex<HashMap<BlockId, i32>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LockAbortException;
impl fmt::Display for LockAbortException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lock abort")
    }
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    pub fn s_lock(&mut self, blk: &BlockId) -> Result<(), LockAbortException> {
        let timestamp = Utc::now().timestamp_millis();
        while self.has_x_lock(blk).map_err(|_| LockAbortException)?
            && !Self::waiting_too_long(timestamp)
        {
            thread::sleep(time::Duration::from_millis(MAX_TIME.try_into().unwrap()));
        }
        if self.has_x_lock(blk).map_err(|_| LockAbortException)? {
            return Err(From::from(LockAbortException));
        }
        let val = self.get_lock_val(blk).map_err(|_| LockAbortException)?;
        self.locks
            .lock()
            .map_err(|_| LockAbortException)?
            .insert(blk.clone(), val + 1);

        Ok(())
    }

    pub fn x_lock(&self, blk: &BlockId) -> Result<(), LockAbortException> {
        let timestamp = Utc::now().timestamp_millis();
        while self
            .has_other_s_locks(blk)
            .map_err(|_| LockAbortException)?
            && !Self::waiting_too_long(timestamp)
        {
            thread::sleep(time::Duration::from_millis(MAX_TIME.try_into().unwrap()));
        }
        if self
            .has_other_s_locks(blk)
            .map_err(|_| LockAbortException)?
        {
            return Err(From::from(LockAbortException));
        }
        self.locks
            .lock()
            .map_err(|_| LockAbortException)?
            .insert(blk.clone(), -1);

        Ok(())
    }

    pub(crate) fn unlock(&self, blk: &BlockId) -> Result<(), String> {
        let val = self.get_lock_val(blk)?;
        if val > 1 {
            self.locks
                .lock()
                .map_err(|_| "failed to get lock")?
                .insert(blk.clone(), val - 1);
        } else {
            self.locks
                .lock()
                .map_err(|_| "failed to get lock")?
                .remove(blk);
        }
        Ok(())
    }

    fn has_x_lock(&self, blk: &BlockId) -> Result<bool, String> {
        Ok(self.get_lock_val(blk)? < 0)
    }

    fn has_other_s_locks(&self, blk: &BlockId) -> Result<bool, String> {
        Ok(self.get_lock_val(blk)? > 1)
    }

    fn get_lock_val(&self, blk: &BlockId) -> Result<i32, String> {
        Ok(*self
            .locks
            .lock()
            .map_err(|_| "failed to get lock")?
            .get(blk)
            .unwrap_or(&0))
    }
    fn waiting_too_long(starttime: i64) -> bool {
        Utc::now().timestamp_millis() - starttime > MAX_TIME
    }
}
