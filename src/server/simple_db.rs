use std::{cell::RefCell, path::Path, rc::Rc};

use crate::{
    buffer::buffer_manager::BufferManager, constants::LOG_FILE, file::file_manager::FileManager,
    log::log_mgr::LogMgr,
};

pub struct SimpleDB {
    fm: Rc<RefCell<FileManager>>,
    lm: Rc<RefCell<LogMgr>>,
    bm: Rc<RefCell<BufferManager>>,
}

impl SimpleDB {
    pub fn new(dirname: &Path, blocksize: i32, buffsize: i32) -> Self {
        let fm = Rc::new(RefCell::new(FileManager::new_from_blocksize(
            &dirname, blocksize,
        )));
        let lm = Rc::new(RefCell::new(LogMgr::new(fm.clone(), LOG_FILE.to_string())));
        let bm = Rc::new(RefCell::new(BufferManager::new(
            fm.clone(),
            lm.clone(),
            buffsize,
        )));
        Self { fm, lm, bm }
    }

    pub fn log_mgr(&self) -> Rc<RefCell<LogMgr>> {
        self.lm.clone()
    }

    pub fn buffer_manager(&self) -> Rc<RefCell<BufferManager>> {
        self.bm.clone()
    }
}
