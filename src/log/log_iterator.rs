use std::sync::Arc;

use crate::{
    constants::INTEGER_BYTES,
    file::{block_id::BlockId, file_manager::FileManager, page::Page},
};

pub struct LogIterator {
    fm: Arc<FileManager>,
    blk: BlockId,
    p: Page,
    current_pos: usize,
    boundary: usize,
}

impl Iterator for LogIterator {
    type Item = Result<Vec<u8>, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let block_size = self.fm.block_size() as usize;

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
        match self.p.get_bytes(self.current_pos) {
            Ok(rec) => {
                self.current_pos += INTEGER_BYTES as usize + rec.len();
                return Some(Ok(rec));
            }
            Err(_) => Some(Err("failed to get bytes".to_string())),
        }
    }
}

impl LogIterator {
    pub fn new(fm: Arc<FileManager>, blk: BlockId) -> Result<Self, String> {
        let mut p = Page::new_from_blocksize(fm.block_size() as usize);
        let _ = fm.read(&blk, &mut p);
        let boundary = p.get_int(0).unwrap() as usize;
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
            .read(&blk, &mut self.p);
        self.boundary = self.p.get_int(0).unwrap() as usize;
        self.current_pos = self.boundary;
        Ok(())
    }
}
