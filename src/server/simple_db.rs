use std::{cell::RefCell, rc::Rc};

use crate::{constants::LOG_FILE, file::file_manager::FileManager, log::log_mgr::LogMgr};

pub struct SimpleDB {
    fm: Rc<RefCell<FileManager>>,
    lm: Rc<RefCell<LogMgr>>,
}

impl SimpleDB {
    pub fn new(dirname: String, blocksize: i32, buffsize: i32) -> Self {
        let fm = Rc::new(RefCell::new(FileManager::new_from_blocksize(
            dirname, blocksize,
        )));
        let lm = Rc::new(RefCell::new(LogMgr::new(fm.clone(), LOG_FILE.to_string())));
        Self { fm, lm }
    }

    pub fn log_mgr(&self) -> Rc<RefCell<LogMgr>> {
        self.lm.clone()
    }
}
