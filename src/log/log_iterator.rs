use std::{cell::RefCell, rc::Rc};

use crate::{
    constants::INTEGER_BYTES,
    file::{block_id::BlockId, file_manager::FileManager, page::Page},
};

pub struct LogIterator {
    fm: Rc<RefCell<FileManager>>,
    blk: BlockId,
    p: Page,
    current_pos: usize,
    boundary: usize,
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if !(self.current_pos < self.fm.borrow().block_size().try_into().unwrap()
            || self.blk.number() > 0)
        {
            return None;
        }
        if self.current_pos == self.fm.borrow().block_size() as usize {
            self.blk = BlockId::new(self.blk.file_name(), self.blk.number() - 1);
            self.move_to_block(self.blk.clone());
        }
        let rec = self.p.get_bytes(self.current_pos);
        self.current_pos += INTEGER_BYTES + rec.len();
        return Some(rec);
    }
}

impl LogIterator {
    pub fn new(fm: Rc<RefCell<FileManager>>, blk: BlockId) -> Self {
        let mut p = Page::new_from_blocksize(fm.borrow().block_size() as usize);
        let _ = fm.borrow_mut().read(&blk, &mut p);
        let boundary = p.get_int(0).unwrap() as usize;
        Self {
            fm,
            blk,
            p: p,
            current_pos: boundary,
            boundary,
        }
    }

    fn move_to_block(&mut self, blk: BlockId) {
        let _ = self.fm.borrow_mut().read(&blk, &mut self.p);
        self.boundary = self.p.get_int(0).unwrap() as usize;
        self.current_pos = self.boundary;
    }
}
