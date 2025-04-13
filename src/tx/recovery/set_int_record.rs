use std::sync::{Arc, Mutex};

use crate::{
    constants::INTEGER_BYTES,
    file::{block_id::BlockId, page::Page},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
};

use super::log_record::{self, LogRecord};

pub struct SetIntRecord {
    txnum: i32,
    offset: usize,
    val: i32,
    blk: BlockId,
}

impl LogRecord for SetIntRecord {
    fn op(&self) -> i32 {
        log_record::SETINT
    }

    fn tx_number(&self) -> i32 {
        self.txnum
    }

    fn undo(&self, tx: Arc<Mutex<Transaction>>) -> Result<(), String> {
        let tx = tx.lock().map_err(|_| "failed to get lock")?;
        tx.pin(&self.blk)?;
        tx.set_int(&self.blk, self.offset, self.val, false)?;
        tx.unpin(&self.blk)?;
        Ok(())
    }
}

impl std::fmt::Display for SetIntRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<SETINT {} {} {} {} >",
            self.txnum, self.blk, self.offset, self.val
        )?;
        Ok(())
    }
}

impl SetIntRecord {
    pub fn new_from_page(p: Page) -> Result<Self, String> {
        let tpos = INTEGER_BYTES as usize;
        let txnum = p.get_int(tpos)?;

        let fpos = tpos + INTEGER_BYTES as usize;
        let filename = p.get_string(fpos)?;

        let bpos = fpos + Page::max_length(filename.len());
        let blknum = p.get_int(bpos)?;
        let blk = BlockId::new(filename, blknum);

        let opos = bpos + INTEGER_BYTES as usize;
        let offset = p.get_int(opos)? as usize;

        let vpos = opos + INTEGER_BYTES as usize;
        let val = p.get_int(vpos)?;

        Ok(Self {
            txnum,
            offset,
            val,
            blk,
        })
    }

    pub fn write_to_log(
        lm: Arc<Mutex<LogManager>>,
        txnum: i32,
        blk: &BlockId,
        offset: i32,
        val: i32,
    ) -> Result<i32, String> {
        let tpos = INTEGER_BYTES as usize;
        let fpos = tpos + INTEGER_BYTES as usize;
        let bpos = fpos + Page::max_length(blk.file_name().len());
        let opos = bpos + INTEGER_BYTES as usize;
        let vpos = opos + INTEGER_BYTES as usize;

        let mut p = Page::new_from_blocksize(vpos + INTEGER_BYTES as usize);
        p.set_int(0, log_record::SETINT)?;
        p.set_int(tpos, txnum)?;
        p.set_string(fpos, &blk.file_name())?;
        p.set_int(bpos, blk.number())?;
        p.set_int(opos, offset)?;
        p.set_int(vpos, val)?;
        lm.lock().map_err(|_| "failed to get lock")?.append(
            p.contents()
                .lock()
                .map_err(|_| "failed to get lock")?
                .to_vec(),
        )
    }
}
