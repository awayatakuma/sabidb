use core::time;
use std::{
    fmt,
    sync::{Arc, Mutex},
    thread,
};

use chrono::Utc;

use crate::{
    file::{block_id::BlockId, file_manager::FileManager},
    log::log_manager::LogManager,
};

use super::buffer::Buffer;

const MAX_TIME: i64 = 10000;

#[derive(Debug, Clone, PartialEq)]

pub struct BufferAbortException;
impl fmt::Display for BufferAbortException {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "buffer abort")
    }
}

impl std::error::Error for BufferAbortException {}

#[derive(Debug, Clone)]
pub struct BufferManager {
    bufferpool: Vec<Arc<Mutex<Buffer>>>,
    num_available: i32,
}

impl BufferManager {
    pub fn new(
        fm: Arc<Mutex<FileManager>>,
        lm: Arc<Mutex<LogManager>>,
        numbuffer: i32,
    ) -> Result<Self, String> {
        let mut bufferpool = Vec::<Arc<Mutex<Buffer>>>::new();
        for _ in 0..numbuffer {
            bufferpool.push(Arc::new(Mutex::new(Buffer::new(fm.clone(), lm.clone())?)));
        }
        Ok(Self {
            bufferpool,
            num_available: numbuffer,
        })
    }

    pub fn available(&self) -> i32 {
        self.num_available
    }

    pub fn flush_all(&mut self, txnum: i32) -> Result<(), String> {
        for buff in &mut self.bufferpool {
            let mut locked_buff = buff.lock().map_err(|_| "failed to get lock")?;
            if locked_buff.modifying_tx() == txnum {
                locked_buff.flush()?
            }
        }
        Ok(())
    }

    pub fn unpin(&mut self, buff: Arc<Mutex<Buffer>>) -> Result<(), String> {
        let mut locked_buff = buff.lock().map_err(|_| "failed to get lock")?;
        locked_buff.unpin();
        if !locked_buff.is_pinned() {
            self.num_available += 1;
        }
        Ok(())
    }

    pub fn pin(
        &mut self,
        blk: &BlockId,
    ) -> Result<Option<Arc<Mutex<Buffer>>>, BufferAbortException> {
        let timestamp = Utc::now().timestamp_millis();
        let mut buff = self.try_to_pin(&blk).map_err(|_| BufferAbortException)?;
        while buff.is_none() && !Self::waiting_too_long(timestamp) {
            thread::sleep(time::Duration::from_millis(MAX_TIME.try_into().unwrap()));
            buff = self.try_to_pin(&blk).map_err(|_| BufferAbortException)?;
        }
        if buff.is_none() {
            return Err(From::from(BufferAbortException));
        }
        Ok(buff)
    }

    fn waiting_too_long(starttime: i64) -> bool {
        Utc::now().timestamp_millis() - starttime > MAX_TIME
    }

    fn try_to_pin(&mut self, blk: &BlockId) -> Result<Option<Arc<Mutex<Buffer>>>, String> {
        let buff: Option<Arc<Mutex<Buffer>>> = {
            if let Some(buffer) = self.find_existing_buffer(blk)? {
                Some(buffer)
            } else {
                if let Some(new_buff) = self.choose_unpinned_buffer()? {
                    new_buff
                        .lock()
                        .map_err(|_| "failed to get lock")?
                        .assign_to_block(blk)?;
                    Some(new_buff)
                } else {
                    None
                }
            }
        };
        if let Some(ref b) = buff {
            let mut locked_buff = b.lock().map_err(|_| "failed to get lock")?;
            if !locked_buff.is_pinned() {
                self.num_available -= 1;
            }
            locked_buff.pin();
        }

        Ok(buff)
    }

    fn find_existing_buffer(&self, blk: &BlockId) -> Result<Option<Arc<Mutex<Buffer>>>, String> {
        for buff in &self.bufferpool {
            if let Some(b) = buff.lock().map_err(|_| "failed to get lock")?.block() {
                if b.eq(&blk) {
                    return Ok(Some(buff.clone()));
                }
            }
        }
        Ok(None)
    }

    fn choose_unpinned_buffer(&self) -> Result<Option<Arc<Mutex<Buffer>>>, String> {
        for buff in &self.bufferpool {
            if !buff.lock().map_err(|_| "failed to get lock")?.is_pinned() {
                return Ok(Some(buff.clone()));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::server::simple_db::SimpleDB;

    #[test]
    fn test_main() {
        // This test will take 10 secs to verify BlockAbortException

        let db = SimpleDB::new(&Path::new("/tmp/buffertest"), 400, 3);
        let bm = db.buffer_manager();

        let mut buffs = vec![Ok(None); 6];
        buffs[0] = bm
            .lock()
            .unwrap()
            .pin(Some(&BlockId::new("testfile".to_string(), 0)).unwrap());
        buffs[1] = bm
            .lock()
            .unwrap()
            .pin(Some(&BlockId::new("testfile".to_string(), 1)).unwrap());
        buffs[2] = bm
            .lock()
            .unwrap()
            .pin(Some(&BlockId::new("testfile".to_string(), 2)).unwrap());

        bm.lock()
            .unwrap()
            .unpin(buffs[1].clone().unwrap().unwrap())
            .unwrap();
        buffs[1] = Ok(None);

        buffs[3] = bm
            .lock()
            .unwrap()
            .pin(Some(&BlockId::new("testfile".to_string(), 0)).unwrap());
        buffs[4] = bm
            .lock()
            .unwrap()
            .pin(Some(&BlockId::new("testfile".to_string(), 1)).unwrap());
        println!("Available buffers: {}", bm.lock().unwrap().available());

        println!("Attempting to pin block 3...");
        let res = bm
            .lock()
            .unwrap()
            .pin(Some(&BlockId::new("testfile".to_string(), 3)).unwrap());

        assert_eq!(res.unwrap_err(), BufferAbortException);

        bm.lock()
            .unwrap()
            .unpin(buffs[2].clone().unwrap().unwrap())
            .unwrap();
        buffs[2] = Ok(None);

        buffs[5] = bm
            .lock()
            .unwrap()
            .pin(Some(&BlockId::new("testfile".to_string(), 3)).unwrap());

        println!("Final Buffer Allocation:");

        for (i, buff) in buffs.iter().enumerate() {
            if let Some(b) = buff.clone().unwrap() {
                println!(
                    "buff[{}] pinned to block: {:#?}",
                    i,
                    b.lock().unwrap().block().unwrap()
                )
            }
        }
    }
}
