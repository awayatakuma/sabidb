use std::{cell::RefCell, rc::Rc};

use crate::{
    file::{block_id::BlockId, file_manager::FileManager, page::Page},
    log::log_mgr::LogMgr,
};

pub struct Buffer {
    fm: Rc<RefCell<FileManager>>,
    lm: Rc<RefCell<LogMgr>>,
    contents: Rc<RefCell<Page>>,
    blk: Option<BlockId>,
    pins: i32,
    txnum: i32,
    lsn: i32,
}

impl Buffer {
    pub fn new(fm: Rc<RefCell<FileManager>>, lm: Rc<RefCell<LogMgr>>) -> Self {
        let page = Page::new_from_blocksize(fm.borrow().block_size() as usize);
        Self {
            fm,
            lm,
            contents: Rc::new(RefCell::new(page)),
            blk: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        }
    }

    pub fn contents(&self) -> Rc<RefCell<Page>> {
        self.contents.clone()
    }

    pub fn block(&self) -> Option<BlockId> {
        self.blk.clone()
    }

    pub fn set_modified(&mut self, txmum: i32, lsn: i32) {
        self.txnum = txmum;
        if lsn >= 0 {
            self.lsn = lsn;
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> i32 {
        self.txnum
    }

    pub(crate) fn assign_to_block(&mut self, b: &BlockId) {
        self.flush();
        self.blk = Some(b.clone());
        let _ = self
            .fm
            .borrow_mut()
            .read(b, &mut self.contents.borrow_mut());
        self.pins = 0;
    }

    pub(crate) fn flush(&mut self) {
        if self.txnum >= 0 {
            self.lm.borrow_mut().flush(self.lsn);
            let _ = self
                .fm
                .borrow_mut()
                .write(&self.blk.clone().unwrap(), &self.contents.as_ref().borrow());
        }
    }

    pub(crate) fn pin(&mut self) {
        self.pins += 1;
    }

    pub(crate) fn unpin(&mut self) {
        self.pins -= 1;
    }
}
