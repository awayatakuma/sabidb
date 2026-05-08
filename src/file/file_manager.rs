use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{create_dir, remove_file, File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::file::block_id::BlockId;
use crate::file::page::Page;

#[derive(Debug)]
pub struct FileManager {
    blocksize: i32,
    db_directory: PathBuf,
    open_files: Mutex<HashMap<String, Arc<File>>>,
    is_new: bool,
}

impl FileManager {
    pub fn new_from_blocksize(db_directory: &Path, blocksize: i32) -> Self {
        let is_new = !db_directory.exists()
            || !db_directory.read_dir().unwrap().into_iter().any(|entry| {
                entry.is_ok_and(|e| {
                    let name = e.file_name();
                    name != OsStr::new(".") && name != OsStr::new("..")
                })
            });
        if is_new {
            let _ = create_dir(db_directory);
        }

        for file in db_directory.read_dir().unwrap().into_iter() {
            let filepath = file.unwrap().file_name();
            if filepath.to_string_lossy().starts_with("temp") {
                let _ = remove_file(db_directory.join(filepath));
            }
        }

        Self {
            blocksize,
            db_directory: db_directory.to_path_buf(),
            open_files: Mutex::new(HashMap::new()),
            is_new,
        }
    }

    pub fn read(&self, blk: &BlockId, p: &mut Page) -> Result<(), String> {
        let blocksize = self.blocksize;
        let binding = p.contents();
        let mut contents = binding.lock().map_err(|_| "failed to get lock")?;

        let f = self.get_file(&blk.file_name())?;
        let offset = (blk.number() * blocksize) as u64;

        // Zero-fill the buffer first to handle potential short reads beyond EOF
        contents.fill(0);

        match f.read_at(contents.as_mut_slice(), offset) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!(
                "failed to read block {} at offset {}: {}",
                blk, offset, e
            )),
        }
    }

    pub fn write(&self, blk: &BlockId, p: &Page) -> Result<(), String> {
        let blocksize = self.blocksize;
        let binding = p.contents();
        let contents = binding.lock().map_err(|_| "failed to get lock")?;

        let f = self.get_file(&blk.file_name())?;
        f.write_all_at(contents.as_slice(), (blk.number() * blocksize) as u64)
            .map_err(|e| format!("failed to write content: {}", e))?;
        Ok(())
    }

    pub fn append(&self, filename: &String) -> Result<BlockId, String> {
        let blocksize = self.blocksize;
        let newblknum = self.len(filename)?;
        let blk = BlockId::new(filename.clone(), newblknum);
        let b = vec![0u8; blocksize as usize];

        let f = self.get_file(&blk.file_name())?;
        f.write_all_at(b.as_slice(), (blk.number() * blocksize) as u64)
            .map_err(|e| format!("failed to append content: {}", e))?;

        Ok(blk)
    }

    pub fn len(&self, filename: &String) -> Result<i32, String> {
        let blocksize = self.blocksize;

        let f = self.get_file(filename)?;
        let len = f
            .metadata()
            .map_err(|_| "failed to access file's metadata")?
            .len() as i32
            / blocksize;
        return Ok(len);
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn block_size(&self) -> i32 {
        self.blocksize
    }

    fn get_file(&self, file_name: &str) -> Result<Arc<File>, String> {
        let mut open_files = self.open_files.lock().map_err(|_| "failed to get lock")?;
        if let Some(f) = open_files.get(file_name) {
            return Ok(f.clone());
        }

        let f = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(self.db_directory.join(file_name))
                .map_err(|e| format!("failed to open file {}: {}", file_name, e))?,
        );
        open_files.insert(file_name.to_string(), f.clone());
        Ok(f)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile::TempDir;

    fn setup() -> (FileManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let fm = FileManager::new_from_blocksize(temp_dir.path(), 100);
        (fm, temp_dir)
    }

    #[test]
    fn test_new_from_blocksize() {
        let (fm, _temp_dir) = setup();
        assert_eq!(fm.block_size(), 100);
    }

    #[test]
    fn test_read_write() {
        let filename = "temptest.db".to_string();
        let blk = BlockId::new(filename.clone(), 0);
        let mut page = Page::new_from_blocksize(104);
        page.set_bytes(0, &vec![1u8; 100]).unwrap();

        let (fm, _dir) = setup();

        // Write
        fm.write(&blk, &page).unwrap();

        // Read
        let mut read_page = Page::new_from_blocksize(104);
        fm.read(&blk, &mut read_page).unwrap();

        assert_eq!(
            page.contents().lock().unwrap().to_vec(),
            read_page.contents().lock().unwrap().to_vec()
        );
    }

    #[test]
    fn test_append() {
        let filename = "temptest.db".to_string();
        let (fm, _dir) = setup();

        fm.append(&filename).unwrap();
        assert_eq!(fm.len(&filename).unwrap(), 1);

        fm.append(&filename).unwrap();
        assert_eq!(fm.len(&filename).unwrap(), 2);
    }

    #[test]
    fn test_len() {
        let (fm, _dir) = setup();
        let filename = "temptest.db".to_string();
        // let fm = db.file_manager();
        assert_eq!(fm.len(&filename).unwrap(), 0);

        fm.append(&filename).unwrap();
        assert_eq!(fm.len(&filename).unwrap(), 1);
    }

    #[test]
    fn test_get_file() {
        let (fm, dir) = setup();
        let filename = "temptest.db".to_string();

        fm.get_file(&filename).unwrap();

        assert!(dir.path().join(&filename).exists());
    }
}
