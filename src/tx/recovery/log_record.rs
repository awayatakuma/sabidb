use std::sync::{Arc, Mutex};

use crate::{file::page::Page, tx::transaction::Transaction};

use super::{
    checkpoint_record::CheckpointRecord, commit_record::CommitRecord,
    rollback_record::RollbackRecord, set_int_record::SetIntRecord,
    set_string_record::SetStringRecord, start_record::StartRecord,
};

pub const CHECKPOINT: i32 = 0;
pub const START: i32 = 1;
pub const COMMIT: i32 = 2;
pub const ROLLBACK: i32 = 3;
pub const SETINT: i32 = 4;
pub const SETSTRING: i32 = 5;

pub trait LogRecord {
    fn op(&self) -> i32;
    fn tx_number(&self) -> i32;
    fn undo(&self, tx: Arc<Mutex<Transaction>>) -> Result<(), String>;
}

pub fn create_log_record(bytes: Vec<u8>) -> Result<Box<dyn LogRecord>, String> {
    let p = Page::new_from_bytes(bytes);
    match p.get_int(0)? {
        CHECKPOINT => Ok(Box::new(CheckpointRecord::new())),
        START => Ok(Box::new(StartRecord::new_from_page(p)?)),
        COMMIT => Ok(Box::new(CommitRecord::new_from_page(p)?)),
        ROLLBACK => Ok(Box::new(RollbackRecord::new_from_page(p)?)),
        SETINT => Ok(Box::new(SetIntRecord::new_from_page(p)?)),
        SETSTRING => Ok(Box::new(SetStringRecord::new_from_page(p)?)),
        _ => panic!("unreachable!!"),
    }
}
