use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::buffer_manager::BufferManager, constants::LOG_FILE, file::file_manager::FileManager,
    log::log_manager::LogManager, tx::transaction::Transaction,
};

pub struct SimpleDB {
    fm: Arc<Mutex<FileManager>>,
    lm: Arc<Mutex<LogManager>>,
    bm: Arc<Mutex<BufferManager>>,
}

impl SimpleDB {
    pub fn new(dirname: &Path, blocksize: i32, buffsize: i32) -> Self {
        let fm = Arc::new(Mutex::new(FileManager::new_from_blocksize(
            &dirname, blocksize,
        )));
        let lm = Arc::new(Mutex::new(
            LogManager::new(fm.clone(), LOG_FILE.to_string()).unwrap(),
        ));
        let bm = Arc::new(Mutex::new(
            BufferManager::new(fm.clone(), lm.clone(), buffsize).unwrap(),
        ));
        Self { fm, lm, bm }
    }
    pub fn file_manager(&self) -> Arc<Mutex<FileManager>> {
        self.fm.clone()
    }

    pub fn log_mgr(&self) -> Arc<Mutex<LogManager>> {
        self.lm.clone()
    }

    pub fn buffer_manager(&self) -> Arc<Mutex<BufferManager>> {
        self.bm.clone()
    }

    pub fn new_tx(&self) -> Arc<Mutex<Transaction>> {
        Arc::new(Mutex::new(
            Transaction::new_from_managers(self.fm.clone(), self.lm.clone(), self.bm.clone())
                .unwrap(),
        ))
    }
}
