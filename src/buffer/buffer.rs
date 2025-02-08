use std::sync::{Arc, Mutex};

use crate::{
    file::{block_id::BlockId, file_manager::FileManager, page::Page},
    log::log_manager::LogManager,
};

#[derive(Debug)]
pub struct Buffer {
    fm: Arc<Mutex<FileManager>>,
    lm: Arc<Mutex<LogManager>>,
    contents: Page,
    blk: Option<BlockId>,
    pins: i32,
    txnum: i32,
    lsn: i32,
}

impl Buffer {
    pub fn new(fm: Arc<Mutex<FileManager>>, lm: Arc<Mutex<LogManager>>) -> Result<Self, String> {
        let page = Page::new_from_blocksize(
            fm.lock().map_err(|_| "failed to get lock")?.block_size() as usize,
        );
        Ok(Self {
            fm,
            lm,
            contents: page,
            blk: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        })
    }

    pub fn contents(&mut self) -> &mut Page {
        &mut self.contents
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

    pub(crate) fn assign_to_block(&mut self, b: &BlockId) -> Result<(), String> {
        self.flush()?;
        let mut fm = self.fm.lock().map_err(|_| "failed to get lock")?;
        self.blk = Some(b.clone());
        fm.read(b, &mut self.contents)?;
        self.pins = 0;
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<(), String> {
        if self.txnum >= 0 {
            let mut lm = self.lm.lock().map_err(|_| "failed to get lock")?;
            lm.flush(self.lsn)?;
            let mut fm = self.fm.lock().map_err(|_| "failed to get lock")?;
            fm.write(&self.blk.clone().unwrap(), &self.contents)
                .map_err(|_| "failed to write")?
        }
        Ok(())
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
        // This test will take 10 secs
        let db = SimpleDB::new(&Path::new("/tmp/buffertest"), 400, 3);
        let bm = db.buffer_manager();
        let mut bm = bm.lock().unwrap();

        let buff1 = bm
            .pin(&BlockId::new("testfile".to_string(), 1))
            .unwrap()
            .unwrap();
        let n = buff1.lock().unwrap().contents().get_int(80).unwrap();
        buff1.lock().unwrap().contents().set_int(80, n + 1);
        // p.borrow_mut().set_string(85, &"hello".to_string());
        buff1.lock().unwrap().set_modified(1, 0);
        println!("The new value is {}", n + 1);
        bm.unpin(buff1).unwrap();

        let mut buff2 = bm
            .pin(&BlockId::new("testfile".to_string(), 2))
            .unwrap()
            .unwrap();
        bm.pin(&BlockId::new("testfile".to_string(), 3)).unwrap();
        bm.pin(&BlockId::new("testfile".to_string(), 4)).unwrap();

        bm.unpin(buff2).unwrap();
        buff2 = bm
            .pin(&BlockId::new("testfile".to_string(), 1))
            .unwrap()
            .unwrap();
        buff2.lock().unwrap().contents().set_int(80, 9999);
        buff2.lock().unwrap().set_modified(1, 0);
    }

    #[test]
    fn test_file_main() {
        let db = SimpleDB::new(&Path::new("/tmp/buffertest"), 400, 8);
        let bm = db.buffer_manager();

        let blk = BlockId::new("testfile".to_string(), 2);
        let pos1 = 88;

        let b1: Arc<Mutex<Buffer>> = bm.lock().unwrap().pin(&blk).unwrap().unwrap();
        b1.lock()
            .unwrap()
            .contents()
            .set_string(pos1, &"abcdefghijklm".to_string());
        let size = Page::max_length("abcdefghijklm".to_string().len());
        let pos2 = pos1 + size;
        b1.lock().unwrap().contents().set_int(pos2, 345);
        b1.lock().unwrap().set_modified(1, 0);
        bm.lock().unwrap().unpin(b1).unwrap();

        let b2 = bm.lock().unwrap().pin(&blk).unwrap().unwrap();
        println!(
            "offset {} contents {}",
            pos2,
            b2.lock().unwrap().contents().get_int(pos2).unwrap()
        );
        println!(
            "offset {} contents {}",
            pos1,
            b2.lock().unwrap().contents().get_string(pos1).unwrap()
        );
        bm.lock().unwrap().unpin(b2.clone()).unwrap();

        assert_eq!(345, b2.lock().unwrap().contents().get_int(pos2).unwrap());
        assert_eq!(
            "abcdefghijklm".to_string(),
            b2.lock().unwrap().contents().get_string(pos1).unwrap()
        );
    }
}
