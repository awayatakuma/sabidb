use core::fmt;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::file::block_id::BlockId;

pub struct LockTable {
    pub(crate) locks: Arc<Mutex<HashMap<BlockId, i32>>>,
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

    pub(crate) fn has_x_lock(&self, blk: &BlockId) -> Result<bool, String> {
        Ok(self.get_lock_val(blk)? < 0)
    }

    pub(crate) fn has_other_s_locks(&self, blk: &BlockId) -> Result<bool, String> {
        Ok(self.get_lock_val(blk)? > 1)
    }

    pub(crate) fn get_lock_val(&self, blk: &BlockId) -> Result<i32, String> {
        Ok(*self
            .locks
            .lock()
            .map_err(|_| "failed to get lock")?
            .get(blk)
            .unwrap_or(&0))
    }
}
