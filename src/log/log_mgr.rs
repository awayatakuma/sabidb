use std::{cell::RefCell, rc::Rc};

use crate::{
    constants::INTEGER_BYTES,
    file::{block_id::BlockId, file_manager::FileManager, page::Page},
};

use super::log_iterator::LogIterator;

pub struct LogMgr {
    fm: Rc<RefCell<FileManager>>,
    logfile: String,
    logpage: Page,
    current_blk: BlockId,
    latest_lsn: i64,
    last_save_lsn: i64,
}

impl LogMgr {
    pub fn new(fm: Rc<RefCell<FileManager>>, logfile: String) -> Self {
        let mut logpage = Page::new_from_blocksize(fm.borrow().block_size() as usize);
        let logsize = fm.borrow_mut().len(&logfile).unwrap();

        let current_blk = if logsize == 0 {
            let blk = fm.borrow_mut().append(&logfile).unwrap();
            logpage.set_int(0, fm.borrow_mut().block_size() as i32);
            let _ = fm.borrow_mut().write(&blk, &logpage);
            blk
        } else {
            let blk = BlockId::new(logfile.clone().to_string(), logsize - 1);
            let _ = fm.borrow_mut().read(&blk, &mut logpage);
            blk
        };
        return LogMgr {
            fm,
            logfile: logfile,
            logpage: logpage,
            current_blk: current_blk,
            latest_lsn: 0,
            last_save_lsn: 0,
        };
    }

    pub fn flush(&mut self, lsn: i64) {
        if lsn >= self.last_save_lsn {
            self.flush_internal();
        }
    }

    pub fn iterator(&mut self) -> LogIterator {
        self.flush_internal();
        return LogIterator::new(self.fm.clone(), self.current_blk.clone());
    }

    pub fn append(&mut self, logrec: Vec<u8>) -> i64 {
        let mut boundary = self.logpage.get_int(0).unwrap() as usize;
        let recsize = logrec.len();
        let bytesneeded = recsize + INTEGER_BYTES;
        if boundary < bytesneeded + INTEGER_BYTES {
            self.flush_internal();
            self.current_blk = self.append_new_block();
            boundary = self.logpage.get_int(0).unwrap() as usize;
        }
        let recpos = boundary - bytesneeded;

        self.logpage.set_bytes(recpos, &logrec);
        self.logpage.set_int(0, recpos as i32);
        self.latest_lsn += 1;
        return self.latest_lsn;
    }

    fn append_new_block(&mut self) -> BlockId {
        let blk = self.fm.borrow_mut().append(&self.logfile).unwrap();
        self.logpage
            .set_int(0, self.fm.borrow_mut().block_size() as i32);
        let _ = self.fm.borrow_mut().write(&blk, &self.logpage);
        blk
    }

    fn flush_internal(&mut self) {
        let _ = self.fm.borrow_mut().write(&self.current_blk, &self.logpage);
        self.last_save_lsn = self.latest_lsn;
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Ref, clone, process::Command};

    use tempfile::TempDir;

    use super::*;
    use crate::{file::file_manager::FileManager, server::simple_db::SimpleDB};

    // Helper function to create a temporary test file manager
    fn create_test_file_manager() -> (Rc<RefCell<FileManager>>, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        let _ = Command::new("touch")
            .arg("test_log.log")
            .current_dir(&temp_dir)
            .status();

        (
            Rc::new(RefCell::new(FileManager::new_from_blocksize(
                temp_dir.path().to_str().unwrap().to_string(),
                400,
            ))),
            temp_dir,
        )
    }

    fn print_log_records(lm: Rc<RefCell<LogMgr>>, msg: String) {
        println!("{}", msg);
        let mut iter = lm.borrow_mut().iterator();
        while let Some(rec) = iter.next() {
            let p = Page::new_from_bytes(rec);
            let s = p.get_string(0).unwrap();
            let npos = Page::max_length(s.len());
            let val = p.get_int(npos).unwrap();
            println!("[ {} , {} ]", s, val)
        }
        println!();

        fn create_records(lm: Rc<RefCell<LogMgr>>, start: i32, end: i32) {
            println!("Creating records:");
            for i in start..end {
                let s = format!("{}{}", "record".to_string(), i.to_string());
                let npos = Page::max_length(s.len());
                let b = vec![0u8; npos + INTEGER_BYTES];
                let mut p = Page::new_from_bytes(b);
                p.set_string(0, &s);
                p.set_int(npos, i);
                let lsm = lm.borrow_mut().append(p.contents().borrow_mut().to_vec());
                print!("{} ", lsm)
            }
            println!()
        }

        #[test]
        fn test_log_mgr_new() {
            let (fm, _temp_dir) = create_test_file_manager();
            let logfile = "test_log.log".to_string();

            let log_mgr = LogMgr::new(fm.clone(), logfile.clone());

            assert_eq!(log_mgr.logfile, logfile);
            assert_eq!(log_mgr.latest_lsn, 0);
            assert_eq!(log_mgr.last_save_lsn, 0);
        }

        #[test]
        fn test_log_mgr_append() {
            let (fm, _temp_dir) = create_test_file_manager();
            let logfile = "test_log.log".to_string();
            let mut log_mgr = LogMgr::new(fm, logfile);

            let log_record1 = vec![1, 2, 3, 4];
            let lsn1 = log_mgr.append(log_record1.clone());

            assert_eq!(lsn1, 1);

            let log_record2 = vec![5, 6, 7, 8];
            let lsn2 = log_mgr.append(log_record2.clone());

            assert_eq!(lsn2, 2);
        }

        #[test]
        fn test_log_mgr_flush() {
            let (fm, _temp_dir) = create_test_file_manager();
            let logfile = "test_log.log".to_string();
            let mut log_mgr = LogMgr::new(fm, logfile);

            let log_record = vec![1, 2, 3, 4];
            let lsn = log_mgr.append(log_record);

            log_mgr.flush(lsn);

            assert_eq!(log_mgr.last_save_lsn, lsn);
        }

        #[test]
        fn test_log_mgr_append_new_block() {
            let (fm, _temp_dir) = create_test_file_manager();
            let logfile = "test_log.log".to_string();
            let mut log_mgr = LogMgr::new(fm, logfile);

            // Fill up the initial block
            let large_record = vec![0; 350]; // Assuming block size is 400

            let initial_block = log_mgr.current_blk.clone();
            log_mgr.append(large_record);

            assert_ne!(log_mgr.current_blk, initial_block);
        }

        #[test]
        fn test_main() {
            let db = SimpleDB::new("/tmp/logtest".to_string(), 400, 8);
            let lm = db.log_mgr();

            print_log_records(lm.clone(), "The initial empty log file:".to_string());
            println!("done");
            create_records(lm.clone(), 1, 35);
            print_log_records(
                lm.clone(),
                "The log file now has these records:".to_string(),
            );
            create_records(lm.clone(), 36, 70);
            lm.borrow_mut().flush(65);
            print_log_records(
                lm.clone(),
                "The log file now has these records:".to_string(),
            );
        }
    }
}
