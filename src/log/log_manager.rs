use std::sync::{Arc, Mutex};

use crate::{
    constants::INTEGER_BYTES,
    file::{block_id::BlockId, file_manager::FileManager, page::Page},
};

use super::log_iterator::LogIterator;

#[derive(Debug)]
pub struct LogManager {
    fm: Arc<Mutex<FileManager>>,
    logfile: String,
    logpage: Page,
    current_blk: BlockId,
    latest_lsn: i32,
    last_save_lsn: i32,
}

impl LogManager {
    pub fn new(fm: Arc<Mutex<FileManager>>, logfile: String) -> Result<Self, String> {
        let mut locked_fm = fm.lock().map_err(|_| "failed to get lock")?;
        let mut logpage = Page::new_from_blocksize(locked_fm.block_size() as usize);
        let logsize = locked_fm.len(&logfile).unwrap();

        let current_blk = if logsize == 0 {
            let blk = locked_fm.append(&logfile).unwrap();
            logpage.set_int(0, locked_fm.block_size() as i32)?;
            let _ = locked_fm.write(&blk, &logpage);
            blk
        } else {
            let blk = BlockId::new(logfile.clone().to_string(), logsize - 1);
            let _ = locked_fm.read(&blk, &mut logpage);
            blk
        };
        drop(locked_fm);
        return Ok(LogManager {
            fm,
            logfile: logfile,
            logpage: logpage,
            current_blk: current_blk,
            latest_lsn: 0,
            last_save_lsn: 0,
        });
    }

    pub fn flush(&mut self, lsn: i32) -> Result<(), String> {
        if lsn >= self.last_save_lsn {
            self.flush_internal()?
        }
        Ok(())
    }

    pub fn iterator(&mut self) -> Result<LogIterator, String> {
        // self.flush_internal();
        // TO-DO: In textbook, this code is needed but I think you cannot match requirement described in p84 if this code remains.
        // So if another problem happens related to this code, I will remove the comment out.
        return LogIterator::new(self.fm.clone(), self.current_blk.clone());
    }

    pub fn append(&mut self, logrec: Vec<u8>) -> Result<i32, String> {
        let mut boundary = self.logpage.get_int(0).unwrap() as usize;
        let recsize = logrec.len();
        let bytesneeded = recsize + INTEGER_BYTES;
        if boundary < bytesneeded + INTEGER_BYTES {
            self.flush_internal()?;
            self.current_blk = self.append_new_block()?;
            boundary = self.logpage.get_int(0).unwrap() as usize;
        }
        let recpos = boundary - bytesneeded;

        self.logpage.set_bytes(recpos, &logrec)?;
        self.logpage.set_int(0, recpos as i32)?;
        self.latest_lsn += 1;
        return Ok(self.latest_lsn);
    }

    fn append_new_block(&mut self) -> Result<BlockId, String> {
        let mut locked_fm = self.fm.lock().map_err(|_| "failed to get lock")?;
        let blk = locked_fm.append(&self.logfile).unwrap();
        self.logpage.set_int(0, locked_fm.block_size() as i32)?;
        let _ = locked_fm.write(&blk, &self.logpage);
        Ok(blk)
    }

    fn flush_internal(&mut self) -> Result<(), String> {
        let _ = self
            .fm
            .lock()
            .map_err(|_| "failed to get lock")?
            .write(&self.current_blk, &self.logpage);
        self.last_save_lsn = self.latest_lsn;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::TempDir;

    use super::*;
    use crate::{file::file_manager::FileManager, server::simple_db::SimpleDB};

    // Helper function to create a temporary test file manager
    fn create_test_file_manager() -> (Arc<Mutex<FileManager>>, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        (
            Arc::new(Mutex::new(FileManager::new_from_blocksize(
                temp_dir.path(),
                400,
            ))),
            temp_dir,
        )
    }

    fn print_log_records(lm: Arc<Mutex<LogManager>>, msg: String) {
        println!("{}", msg);
        let mut iter = lm.lock().unwrap().iterator().unwrap();
        while let Some(rec) = iter.next() {
            let p = Page::new_from_bytes(rec.unwrap());
            let s = p.get_string(0).unwrap();
            let npos = Page::max_length(s.len());
            let val = p.get_int(npos).unwrap();
            println!("[ {} , {} ]", s, val)
        }
        println!();
    }

    fn create_records(lm: Arc<Mutex<LogManager>>, start: i32, end: i32) {
        println!("Creating records:");
        for i in start..=end {
            let s = format!("{}{}", "record".to_string(), i.to_string());
            let npos = Page::max_length(s.len());
            let b = vec![0u8; npos + INTEGER_BYTES];
            let mut p = Page::new_from_bytes(b);
            p.set_string(0, &s).unwrap();
            p.set_int(npos, i).unwrap();
            let _lsm = lm
                .lock()
                .unwrap()
                .append(p.contents().lock().unwrap().to_vec())
                .unwrap();
        }
        println!()
    }

    #[test]
    fn test_log_mgr_new() {
        let (fm, _temp_dir) = create_test_file_manager();
        let logfile = "test_log.log".to_string();

        let log_mgr = LogManager::new(fm.clone(), logfile.clone()).unwrap();

        assert_eq!(log_mgr.logfile, logfile);
        assert_eq!(log_mgr.latest_lsn, 0);
        assert_eq!(log_mgr.last_save_lsn, 0);
    }

    #[test]
    fn test_log_mgr_append() {
        let (fm, _temp_dir) = create_test_file_manager();
        let logfile = "test_log.log".to_string();
        let mut log_mgr = LogManager::new(fm, logfile).unwrap();

        let log_record1 = vec![1, 2, 3, 4];
        let lsn1 = log_mgr.append(log_record1.clone());

        assert_eq!(lsn1.unwrap(), 1);

        let log_record2 = vec![5, 6, 7, 8];
        let lsn2 = log_mgr.append(log_record2.clone());

        assert_eq!(lsn2.unwrap(), 2);
    }

    #[test]
    fn test_log_mgr_flush() {
        let (fm, _temp_dir) = create_test_file_manager();
        let logfile = "test_log.log".to_string();
        let mut log_mgr = LogManager::new(fm, logfile).unwrap();

        let log_record = vec![1, 2, 3, 4];
        let lsn = log_mgr.append(log_record).unwrap();

        log_mgr.flush(lsn).unwrap();

        assert_eq!(log_mgr.last_save_lsn, lsn);
    }

    #[test]
    fn test_log_mgr_append_new_block() {
        let (fm, _temp_dir) = create_test_file_manager();
        let logfile = "test_log.log".to_string();
        let mut log_mgr = LogManager::new(fm, logfile).unwrap();

        // Fill up the initial block
        let large_record = vec![0; 396]; // Assuming block size is 400

        let initial_block = log_mgr.current_blk.clone();
        log_mgr.append(large_record).unwrap();

        assert_ne!(log_mgr.current_blk, initial_block);
    }

    #[test]
    fn test_main() {
        let db = SimpleDB::new(&Path::new("/tmp/logtest"), 400, 8);
        let lm = db.log_mgr();

        print_log_records(lm.clone(), "The initial empty log file:".to_string());
        println!("done");
        create_records(lm.clone(), 1, 35);
        print_log_records(
            lm.clone(),
            "The log file now has these records:".to_string(),
        );
        create_records(lm.clone(), 36, 70);
        lm.lock().unwrap().flush(65).unwrap();
        print_log_records(
            lm.clone(),
            "the log file now has these records:".to_string(),
        );
    }
}
