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

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::server::simple_db::SimpleDB;

    #[test]
    fn test_main() {
        let db = SimpleDB::new(&Path::new("/tmp/buffertest"), 400, 3);
        let bm = db.buffer_manager();

        let buff1 = bm
            .borrow_mut()
            .pin(&BlockId::new("testfile".to_string(), 1))
            .unwrap();
        let p = buff1.borrow().contents();
        let n = p.borrow().get_int(80).unwrap();
        p.borrow_mut().set_int(80, n + 1);
        // p.borrow_mut().set_string(85, &"hello".to_string());
        buff1.borrow_mut().set_modified(1, 0);
        println!("The new value is {}", n + 1);
        bm.borrow_mut().unpin(buff1);

        let mut buff2 = bm
            .borrow_mut()
            .pin(&BlockId::new("testfile".to_string(), 2))
            .unwrap();
        let _ = bm
            .borrow_mut()
            .pin(&BlockId::new("testfile".to_string(), 3))
            .unwrap();
        let _ = bm
            .borrow_mut()
            .pin(&BlockId::new("testfile".to_string(), 4))
            .unwrap();

        bm.borrow_mut().unpin(buff2);
        buff2 = bm
            .borrow_mut()
            .pin(&BlockId::new("testfile".to_string(), 1))
            .unwrap();
        let p2 = buff2.borrow_mut().contents();
        p2.borrow_mut().set_int(80, 9999);
        buff2.borrow_mut().set_modified(1, 0);
    }

    #[test]
    fn test_fime_main() {
        let db = SimpleDB::new(&Path::new("/tmp/buffertest"), 400, 8);
        let bm = db.buffer_manager();

        let blk = BlockId::new("testfile".to_string(), 2);
        let pos1 = 88;

        let b1: Rc<RefCell<Buffer>> = bm.borrow_mut().pin(&blk).unwrap();
        let p1 = b1.borrow_mut().contents();
        p1.borrow_mut()
            .set_string(pos1, &"abcdefghijklm".to_string());
        let size = Page::max_length("abcdefghijklm".to_string().len());
        let pos2 = pos1 + size;
        p1.borrow_mut().set_int(pos2, 345);
        b1.borrow_mut().set_modified(1, 0);
        bm.borrow_mut().unpin(b1);

        let b2 = bm.borrow_mut().pin(&blk).unwrap();
        let p2 = b2.borrow_mut().contents();
        println!(
            "offset {} contents {}",
            pos2,
            p2.borrow().get_int(pos2).unwrap()
        );
        println!(
            "offset {} contents {}",
            pos1,
            p2.borrow().get_string(pos1).unwrap()
        );
        bm.borrow_mut().unpin(b2);

        assert_eq!(345, p2.borrow().get_int(pos2).unwrap());
        assert_eq!(
            "abcdefghijklm".to_string(),
            p2.borrow().get_string(pos1).unwrap()
        );
    }
}
