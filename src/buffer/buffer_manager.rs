use core::time;
use std::{cell::RefCell, rc::Rc, thread};

use chrono::Utc;

use crate::{
    file::{block_id::BlockId, file_manager::FileManager},
    log::log_mgr::LogMgr,
};

use super::buffer::Buffer;

const MAX_TIME: i64 = 10000;

pub struct BufferManager {
    bufferpool: Vec<Rc<RefCell<Buffer>>>,
    num_available: i32,
}

impl BufferManager {
    pub fn new(fm: Rc<RefCell<FileManager>>, lm: Rc<RefCell<LogMgr>>, numbuffer: i32) -> Self {
        let mut bufferpool = Vec::<Rc<RefCell<Buffer>>>::with_capacity(numbuffer as usize);
        for _ in 0..numbuffer {
            bufferpool.push(Rc::new(RefCell::new(Buffer::new(fm.clone(), lm.clone()))));
        }
        Self {
            bufferpool,
            num_available: numbuffer,
        }
    }

    pub fn available(&self) -> i32 {
        self.num_available
    }

    pub fn flush_all(&mut self, txnum: i32) {
        for buff in &mut self.bufferpool {
            if buff.borrow_mut().modifying_tx() == txnum {
                buff.borrow_mut().flush();
            }
        }
    }

    pub fn unpin(&mut self, buff: Rc<RefCell<Buffer>>) {
        buff.borrow_mut().unpin();
        if !buff.borrow().is_pinned() {
            self.num_available += 1;
        }
    }

    pub fn pin(&mut self, blk: &BlockId) -> Option<Rc<RefCell<Buffer>>> {
        let timesptamp = Utc::now().timestamp_millis();
        let mut buff = self.try_to_pin(&blk);
        while buff.is_none() && !Self::waiting_too_long(timesptamp) {
            thread::sleep(time::Duration::from_millis(MAX_TIME.try_into().unwrap()));
            buff = self.try_to_pin(&blk);
        }
        if buff.is_none() {
            // panic!("Buffer Abort Exception")
            println!("Buffer Abort Exception");
            // ToDo: rewrite thi with Result type
            return None;
        }
        buff
    }

    fn waiting_too_long(starttime: i64) -> bool {
        Utc::now().timestamp_millis() - starttime > MAX_TIME
    }

    fn try_to_pin(&mut self, blk: &BlockId) -> Option<Rc<RefCell<Buffer>>> {
        let buff: Option<Rc<RefCell<Buffer>>> = {
            if let Some(buffer) = self.find_existing_buffer(blk) {
                Some(buffer)
            } else {
                if let Some(new_buff) = self.choose_unpinned_buffer() {
                    new_buff.borrow_mut().assign_to_block(blk);
                    Some(new_buff)
                } else {
                    None
                }
            }
        };
        if let Some(ref b) = buff {
            if !b.borrow().is_pinned() {
                self.num_available -= 1;
            }
            b.borrow_mut().pin();
        }

        buff
    }

    fn find_existing_buffer(&self, blk: &BlockId) -> Option<Rc<RefCell<Buffer>>> {
        for buff in &self.bufferpool {
            if let Some(b) = buff.borrow().block() {
                if b.eq(&blk) {
                    return Some(buff.clone());
                }
            }
        }
        None
    }

    fn choose_unpinned_buffer(&self) -> Option<Rc<RefCell<Buffer>>> {
        for buff in &self.bufferpool {
            if !buff.borrow().is_pinned() {
                return Some(buff.clone());
            }
        }
        None
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

        let mut buffs = Vec::new();
        buffs.push(
            bm.borrow_mut()
                .pin(Some(&BlockId::new("testfile".to_string(), 0)).unwrap()),
        );
        buffs.push(
            bm.borrow_mut()
                .pin(Some(&BlockId::new("testfile".to_string(), 1)).unwrap()),
        );
        buffs.push(
            bm.borrow_mut()
                .pin(Some(&BlockId::new("testfile".to_string(), 2)).unwrap()),
        );
        bm.borrow_mut().unpin(buffs[1].clone().unwrap());
        buffs[1] = None;
        buffs.push(
            bm.borrow_mut()
                .pin(Some(&BlockId::new("testfile".to_string(), 3)).unwrap()),
        );
        buffs.push(
            bm.borrow_mut()
                .pin(Some(&BlockId::new("testfile".to_string(), 4)).unwrap()),
        );
        println!("Available buffers: {}", bm.borrow().available());

        println!("Attempting to pin block 3...");
        buffs.push(
            bm.borrow_mut()
                .pin(Some(&BlockId::new("testfile".to_string(), 5)).unwrap()),
        );
        bm.borrow_mut().unpin(buffs[2].clone().unwrap());
        buffs[2] = None;
        buffs[5] = bm
            .borrow_mut()
            .pin(Some(&BlockId::new("testfile".to_string(), 3)).unwrap());
        println!("Final Buffer Allocation:");
        for (i, buff) in buffs.iter().enumerate() {
            if let Some(b) = buff {
                println!(
                    "buff[{}] pinned to block {:#?}",
                    i,
                    b.borrow().block().unwrap()
                )
            }
        }
    }
}
