use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffer_manager::BufferManager,
    file::{block_id::BlockId, file_manager::FileManager},
    log::log_manager::LogManager,
};
use lazy_static::lazy_static;

use super::{
    buffer_list::BufferList, concurrency::concurrency_manager::ConcurrencyManager,
    recovery::recovery_manager::RecoveryManager,
};

const END_OF_FILE: i32 = -1;
lazy_static! {
    static ref next_tx_num: Arc<Mutex<i32>> = Arc::new(Mutex::new(0));
}

#[derive(Debug, Clone)]
pub struct Transaction {
    recovery_manager: Option<Arc<Mutex<RecoveryManager>>>,
    concurrent_manager: Arc<Mutex<ConcurrencyManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    file_manager: Arc<Mutex<FileManager>>,
    txnum: i32,
    mybuffers: Arc<Mutex<BufferList>>,
}

impl Transaction {
    pub fn new_from_managers(
        fm: Arc<Mutex<FileManager>>,
        lm: Arc<Mutex<LogManager>>,
        bm: Arc<Mutex<BufferManager>>,
    ) -> Result<Self, String> {
        let txnum = Self::next_tx_number()?;
        let mut tran = Transaction {
            recovery_manager: None,
            concurrent_manager: Arc::new(Mutex::new(ConcurrencyManager::new())),
            buffer_manager: bm.clone(),
            file_manager: fm,
            txnum: txnum,
            mybuffers: Arc::new(Mutex::new(BufferList::new_from_buffer_manager(bm.clone()))),
        };
        let recovery_manager = Arc::new(Mutex::new(RecoveryManager::new_from_managers(
            Arc::new(Mutex::new(tran.clone())),
            txnum,
            lm,
            bm,
        )?));
        tran.recovery_manager = recovery_manager.into();
        Ok(tran)
    }

    pub fn commit(&mut self) -> Result<(), String> {
        self.recovery_manager
            .as_ref()
            .unwrap()
            .lock()
            .map_err(|_| "failed to get lock")?
            .commit()?;

        println!("transaction {} commited", self.txnum);

        self.concurrent_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .release()?;

        self.mybuffers
            .lock()
            .map_err(|_| "failed to get lock")?
            .unpin_all()?;

        Ok(())
    }

    pub fn rollback(&mut self) -> Result<(), String> {
        self.recovery_manager
            .as_ref()
            .unwrap()
            .lock()
            .map_err(|_| "failed to get lock")?
            .rollback()?;

        println!("transaction {} rolled back", self.txnum);

        self.concurrent_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .release()?;

        self.mybuffers
            .lock()
            .map_err(|_| "failed to get lock")?
            .unpin_all()?;

        Ok(())
    }

    pub fn recover(&self) -> Result<(), String> {
        self.buffer_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .flush_all(self.txnum)?;
        self.recovery_manager
            .as_ref()
            .unwrap()
            .lock()
            .map_err(|_| "failed to get lock")?
            .recover()?;
        Ok(())
    }

    pub fn pin(&self, blk: &BlockId) -> Result<(), String> {
        self.mybuffers
            .lock()
            .map_err(|_| "failed to get lock")?
            .pin(blk)?;
        Ok(())
    }

    pub fn unpin(&self, blk: &BlockId) -> Result<(), String> {
        self.mybuffers
            .lock()
            .map_err(|_| "failed to get lock")?
            .unpin(blk)?;
        Ok(())
    }

    pub fn get_int(&self, blk: &BlockId, offset: usize) -> Result<i32, String> {
        self.concurrent_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .s_lock(blk)?;

        let ret = self
            .mybuffers
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_buffer(blk)
            .ok_or("you access to a buffer that does not exist")?
            .lock()
            .map_err(|_| "failed to get lock")?
            .contents()
            .get_int(offset)?;

        Ok(ret)
    }

    pub fn get_string(&self, blk: &BlockId, offset: usize) -> Result<String, String> {
        self.concurrent_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .s_lock(blk)?;

        let ret = self
            .mybuffers
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_buffer(blk)
            .ok_or("you access to a buffer that does not exist")?
            .lock()
            .map_err(|_| "failed to get lock")?
            .contents()
            .get_string(offset)?;

        Ok(ret)
    }

    pub fn set_int(
        &self,
        blk: &BlockId,
        offset: usize,
        val: i32,
        ok_to_log: bool,
    ) -> Result<(), String> {
        self.concurrent_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .x_lock(blk)?;

        let mut binding = self.mybuffers.lock().map_err(|_| "failed to get lock")?;
        let mut buff = binding
            .get_buffer(blk)
            .ok_or("you access to a buffer that does not exist")?
            .lock()
            .map_err(|_| "failed to get lock")?;

        let lsn = if ok_to_log {
            self.recovery_manager
                .as_ref()
                .unwrap()
                .lock()
                .map_err(|_| "failed to get lock")?
                .set_int(&mut buff, offset as usize, val)?
        } else {
            -1
        };
        let p = buff.contents();
        p.set_int(offset, val)?;
        buff.set_modified(self.txnum, lsn);
        Ok(())
    }

    pub fn set_string(
        &self,
        blk: &BlockId,
        offset: usize,
        val: String,
        ok_to_log: bool,
    ) -> Result<(), String> {
        self.concurrent_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .x_lock(blk)?;

        let mut binding = self.mybuffers.lock().map_err(|_| "failed to get lock")?;
        let mut buff = binding
            .get_buffer(blk)
            .ok_or("you access to a buffer that does not exist")?
            .lock()
            .map_err(|_| "failed to get lock")?;

        let lsn = if ok_to_log {
            self.recovery_manager
                .as_ref()
                .unwrap()
                .lock()
                .map_err(|_| "failed to get lock")?
                .set_string(&mut buff, offset as usize, val.clone())?
        } else {
            -1
        };
        let p = buff.contents();
        p.set_string(offset, &val)?;
        buff.set_modified(self.txnum, lsn);
        Ok(())
    }

    pub fn size(&self, filename: String) -> Result<i32, String> {
        let dummyblk = BlockId::new(filename.clone(), END_OF_FILE);
        self.concurrent_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .s_lock(&dummyblk)?;
        let ret = self
            .file_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .len(&filename)?;
        Ok(ret)
    }

    pub fn append(&self, filename: String) -> Result<BlockId, String> {
        let dummyblk = BlockId::new(filename.clone(), END_OF_FILE);
        self.concurrent_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .x_lock(&dummyblk)?;
        let ret = self
            .file_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .append(&filename)?;
        Ok(ret)
    }

    pub fn block_size(&self) -> Result<i32, String> {
        Ok(self
            .file_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .block_size())
    }

    pub fn available_buffers(&self) -> Result<i32, String> {
        Ok(self
            .buffer_manager
            .lock()
            .map_err(|_| "failed to get lock")?
            .available())
    }

    fn next_tx_number() -> Result<i32, String> {
        let mut num = next_tx_num.lock().map_err(|_| "failed to get lock")?;
        *num += 1;
        Ok(*num)
    }
}
