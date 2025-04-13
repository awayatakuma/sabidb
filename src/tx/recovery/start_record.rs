use std::sync::{Arc, Mutex};

use crate::{
    constants::INTEGER_BYTES, file::page::Page, log::log_manager::LogManager,
    tx::transaction::Transaction,
};

use super::log_record::{self, LogRecord, START};

pub struct StartRecord {
    txnum: i32,
}

impl LogRecord for StartRecord {
    fn op(&self) -> i32 {
        log_record::START
    }

    fn tx_number(&self) -> i32 {
        self.txnum
    }

    fn undo(&self, _tx: Arc<Mutex<Transaction>>) -> Result<(), String> {
        Ok(())
    }
}

impl std::fmt::Display for StartRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<START {}>", self.txnum)?;
        Ok(())
    }
}

impl StartRecord {
    pub fn new_from_page(p: Page) -> Result<Self, String> {
        let tpos = INTEGER_BYTES as usize;
        let txnum = p.get_int(tpos)?;
        Ok(StartRecord { txnum })
    }
    pub fn write_to_log(lm: Arc<Mutex<LogManager>>, txnum: i32) -> Result<i32, String> {
        let mut p = Page::new_from_blocksize(2 * INTEGER_BYTES as usize);
        p.set_int(0, START)?;
        p.set_int(INTEGER_BYTES as usize, txnum)?;
        lm.lock().map_err(|_| "failed to get lock")?.append(
            p.contents()
                .lock()
                .map_err(|_| "failed to get lock")?
                .to_vec(),
        )
    }
}
