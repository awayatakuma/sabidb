use std::sync::{Arc, Mutex};

use crate::{
    constants::INTEGER_BYTES, file::page::Page, log::log_manager::LogManager,
    tx::transaction::Transaction,
};

use super::log_record::{self, LogRecord};

pub struct RollbackRecord {
    txnum: i32,
}

impl LogRecord for RollbackRecord {
    fn op(&self) -> i32 {
        log_record::ROLLBACK
    }

    fn tx_number(&self) -> i32 {
        self.txnum
    }

    fn undo(&self, _tx: Arc<Mutex<Transaction>>) -> Result<(), String> {
        Ok(())
    }
}

impl std::fmt::Display for RollbackRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<ROLLBACK {}>", self.txnum)?;
        Ok(())
    }
}

impl RollbackRecord {
    pub fn new_from_page(p: Page) -> Result<Self, String> {
        let tpos = INTEGER_BYTES;
        let txnum = p.get_int(tpos)?;
        Ok(RollbackRecord { txnum })
    }
    pub fn write_to_log(lm: Arc<Mutex<LogManager>>, txnum: i32) -> Result<i32, String> {
        let mut p = Page::new_from_blocksize(2 * INTEGER_BYTES);
        p.set_int(0, log_record::ROLLBACK)?;
        p.set_int(INTEGER_BYTES, txnum)?;
        lm.lock().map_err(|_| "failed to get lock")?.append(
            p.contents()
                .lock()
                .map_err(|_| "failed to get lock")?
                .to_vec(),
        )
    }
}
