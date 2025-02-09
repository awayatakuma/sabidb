use std::collections::HashMap;
use std::fs::{create_dir, remove_file, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::file::block_id::BlockId;
use crate::file::page::Page;

#[derive(Debug, Clone)]
pub struct FileManager {
    blocksize: i32,
    db_directory: PathBuf,
    open_files: HashMap<String, Arc<Mutex<File>>>,
}

impl FileManager {
    pub fn new_from_blocksize(db_directory: &Path, blocksize: i32) -> Self {
        if !db_directory.exists() {
            let _ = create_dir(db_directory);
        }

        for file in db_directory.iter() {
            if file.to_string_lossy().starts_with("temp") {
                let _ = remove_file(db_directory.join(file));
            }
        }

        Self {
            blocksize,
            db_directory: db_directory.to_path_buf(),
            open_files: HashMap::new(),
        }
    }

    pub fn read(&mut self, blk: &BlockId, p: &mut Page) -> Result<(), String> {
        let blocksize = self.blocksize;
        let binding = p.contents();
        let mut contents = binding.lock().map_err(|_| "failed to get lock")?;

        if let Some(f) = self.get_file(&blk.file_name()) {
            let mut f = f.lock().map_err(|_| "failed to get lock")?;

            f.seek(SeekFrom::Start((blk.number() * blocksize) as u64))
                .map_err(|_| "failed to seek")?;
            f.read(contents.as_mut_slice())
                .map_err(|_| "failed to read")?;

            p.set_contents(contents.clone());
        }
        Ok(())
    }

    pub fn write(&mut self, blk: &BlockId, p: &Page) -> Result<(), String> {
        let blocksize = self.blocksize;

        if let Some(f) = self.get_file(&blk.file_name()) {
            let mut f = f.lock().map_err(|_| "failed to get lock")?;
            f.seek(SeekFrom::Start((blk.number() * blocksize) as u64))
                .map_err(|_| "failed to seek")?;
            f.write_all(
                p.contents()
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .as_slice(),
            )
            .map_err(|_| "failed to write content")?;
        }
        Ok(())
    }

    pub fn append(&mut self, filename: &String) -> Result<BlockId, String> {
        let blocksize = self.blocksize;
        let newblknum = self.len(&filename)?;
        let blk = BlockId::new(filename.clone(), newblknum);
        let b = vec![0u8; blocksize as usize];

        if let Some(f) = self.get_file(&blk.file_name()) {
            let mut f = f.lock().map_err(|_| "failed to get lock")?;
            f.seek(SeekFrom::Start((blk.number() * blocksize) as u64))
                .map_err(|_| "failed to seek")?;
            f.write_all(b.as_slice())
                .map_err(|_| "failed to write content")?;
        }

        Ok(blk)
    }

    pub fn len(&mut self, filename: &String) -> Result<i32, String> {
        let blocksize = self.blocksize;

        let f = self
            .get_file(filename)
            .expect("cannot access to file that does not exist")
            .lock()
            .map_err(|_| "failed to get lock")?;

        let len = f
            .metadata()
            .map_err(|_| "failed to access file's metadata")?
            .len() as i32
            / blocksize;
        return Ok(len);
    }

    pub fn block_size(&self) -> i32 {
        self.blocksize
    }
    fn get_file(&mut self, file_name: &str) -> Option<&mut Arc<Mutex<File>>> {
        let f = self
            .open_files
            .entry(file_name.to_string())
            .or_insert(Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(self.db_directory.join(file_name))
                    .unwrap(),
            )));

        Some(f)
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

        let (mut fm, _dir) = setup();

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
        let (mut fm, _dir) = setup();

        fm.append(&filename).unwrap();
        assert_eq!(fm.len(&filename).unwrap(), 1);

        fm.append(&filename).unwrap();
        assert_eq!(fm.len(&filename).unwrap(), 2);
    }

    #[test]
    fn test_len() {
        let (mut fm, _dir) = setup();
        let filename = "temptest.db".to_string();
        // let fm = db.file_manager();
        assert_eq!(fm.len(&filename).unwrap(), 0);

        fm.append(&filename).unwrap();
        assert_eq!(fm.len(&filename).unwrap(), 1);
    }

    #[test]
    fn test_get_file() {
        let (mut fm, dir) = setup();
        let filename = "temptest.db".to_string();

        fm.get_file(&filename).unwrap();

        assert!(dir.path().join(&filename).exists());
    }
}
