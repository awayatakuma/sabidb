use std::sync::{Arc, Mutex};

use crate::{
    constants::INTEGER_BYTES, file::page::Page, log::log_manager::LogManager,
    tx::transaction::Transaction,
};

use super::log_record::{self, LogRecord, CHECKPOINT};

pub struct CheckpointRecord;

impl LogRecord for CheckpointRecord {
    fn op(&self) -> i32 {
        log_record::CHECKPOINT
    }

    fn tx_number(&self) -> i32 {
        -1
    }

    fn undo(&self, _tx: Arc<Mutex<Transaction>>) -> Result<(), String> {
        Ok(())
    }
}

impl std::fmt::Display for CheckpointRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<CHECKPOINT>",)?;
        Ok(())
    }
}

impl CheckpointRecord {
    pub fn new() -> Self {
        CheckpointRecord
    }
    pub fn write_to_log(lm: Arc<Mutex<LogManager>>) -> Result<i32, String> {
        let mut p = Page::new_from_blocksize(INTEGER_BYTES);
        p.set_int(0, CHECKPOINT)?;
        lm.lock().map_err(|_| "failed to get lock")?.append(
            p.contents()
                .lock()
                .map_err(|_| "failed to get lock")?
                .to_vec(),
        )
    }
}
