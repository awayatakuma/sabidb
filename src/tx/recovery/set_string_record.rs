use std::sync::{Arc, Mutex};

use crate::{
    constants::INTEGER_BYTES,
    file::{block_id::BlockId, page::Page},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
};

use super::log_record::{self, LogRecord};

pub struct SetStringRecord {
    txnum: i32,
    offset: usize,
    val: String,
    blk: BlockId,
}

impl LogRecord for SetStringRecord {
    fn op(&self) -> i32 {
        log_record::SETSTRING
    }

    fn tx_number(&self) -> i32 {
        self.txnum
    }

    fn undo(&self, tx: Arc<Mutex<Transaction>>) -> Result<(), String> {
        let tx = tx.lock().map_err(|_| "failed to get lock")?;
        tx.pin(&self.blk)?;
        tx.set_string(&self.blk, self.offset, self.val.clone(), false)?;
        tx.unpin(&self.blk)?;
        Ok(())
    }
}

impl std::fmt::Display for SetStringRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<SETSTRING {} {} {} {} >",
            self.txnum, self.blk, self.offset, self.val
        )?;
        Ok(())
    }
}

impl SetStringRecord {
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
        let val = p.get_string(vpos)?;

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
        val: String,
    ) -> Result<i32, String> {
        let tpos = INTEGER_BYTES as usize;
        let fpos = tpos + INTEGER_BYTES as usize;
        let bpos = fpos + Page::max_length(blk.file_name().len());
        let opos = bpos + INTEGER_BYTES as usize;
        let vpos = opos + INTEGER_BYTES as usize;
        let reclen = vpos + Page::max_length(val.len());

        let mut p = Page::new_from_blocksize(reclen);
        p.set_int(0, log_record::SETSTRING)?;
        p.set_int(tpos, txnum)?;
        p.set_string(fpos, &blk.file_name())?;
        p.set_int(bpos, blk.number())?;
        p.set_int(opos, offset)?;
        p.set_string(vpos, &val)?;
        lm.lock().map_err(|_| "failed to get lock")?.append(
            p.contents()
                .lock()
                .map_err(|_| "failed to get lock")?
                .to_vec(),
        )
    }
}
