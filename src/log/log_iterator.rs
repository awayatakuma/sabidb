use std::sync::{Arc, Mutex};

use crate::{
    constants::INTEGER_BYTES,
    file::{block_id::BlockId, file_manager::FileManager, page::Page},
};

pub struct LogIterator {
    fm: Arc<Mutex<FileManager>>,
    blk: BlockId,
    p: Page,
    current_pos: usize,
    boundary: usize,
}

impl Iterator for LogIterator {
    type Item = Result<Vec<u8>, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let block_size = match self.fm.lock().map_err(|_| "failed to get lock") {
            Ok(fm) => fm.block_size() as usize,
            Err(e) => return Some(Err(e.to_string())),
        };

        if !(self.current_pos < block_size || self.blk.number() > 0) {
            return None;
        }
        if self.current_pos == block_size {
            self.blk = BlockId::new(self.blk.file_name(), self.blk.number() - 1);
            match self.move_to_block(self.blk.clone()) {
                Ok(()) => (),
                Err(e) => return Some(Err(e.to_string())),
            }
        }
        let rec = self.p.get_bytes(self.current_pos);
        self.current_pos += INTEGER_BYTES + rec.len();
        return Some(Ok(rec));
    }
}

impl LogIterator {
    pub fn new(fm: Arc<Mutex<FileManager>>, blk: BlockId) -> Result<Self, String> {
        let mut locked_fm = fm.lock().map_err(|_| "failed to get lock")?;
        let mut p = Page::new_from_blocksize(locked_fm.block_size() as usize);
        let _ = locked_fm.read(&blk, &mut p);
        let boundary = p.get_int(0).unwrap() as usize;
        drop(locked_fm);
        Ok(Self {
            fm,
            blk,
            p: p,
            current_pos: boundary,
            boundary,
        })
    }

    fn move_to_block(&mut self, blk: BlockId) -> Result<(), String> {
        let _ = self
            .fm
            .lock()
            .map_err(|_| "failed to get lock")?
            .read(&blk, &mut self.p);
        self.boundary = self.p.get_int(0).unwrap() as usize;
        self.current_pos = self.boundary;
        Ok(())
    }
}
