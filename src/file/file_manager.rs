use std::collections::HashMap;
use std::fs::{create_dir, remove_file, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::file::block_id::BlockId;
use crate::file::page::Page;

#[derive(Debug)]
pub struct FileManager {
    blocksize: i32,
    db_directory: PathBuf,
    open_files: Mutex<HashMap<String, File>>,
    mux: Mutex<()>,
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
            open_files: Mutex::new(HashMap::new()),
            mux: Mutex::new(()),
        }
    }

    pub fn read(&mut self, blk: &BlockId, p: &mut Page) -> Result<(), String> {
        let _lock = self.mux.lock().map_err(|_| "Failed to get lock")?;

        let blocksize = self.blocksize;
        let contents = p.contents();

        let mut f = self
            .get_file(&blk.file_name())
            .map_err(|_| "failed to get file")?;
        let _ = f
            .seek(SeekFrom::Start((blk.number() * blocksize) as u64))
            .map_err(|_| "Failed to seek")?;
        let _ = f
            .read(contents.borrow_mut().as_mut_slice())
            .map_err(|_| "Failed to read");
        p.set_contents(contents.borrow_mut().clone());
        Ok(())
    }

    pub fn write(&mut self, blk: &BlockId, p: &Page) -> std::io::Result<()> {
        let blocksize = self.blocksize;
        let mut f = self.get_file(&blk.file_name())?;
        let contents = p.contents();

        f.seek(SeekFrom::Start((blk.number() * blocksize) as u64))?;
        f.write_all(contents.borrow().as_slice())?;

        Ok(())
    }

    pub fn append(&mut self, filename: &String) -> std::io::Result<BlockId> {
        let blocksize = self.blocksize;
        let newblknum = self.len(&filename)?;
        let blk = BlockId::new(filename.clone(), newblknum);
        let b = vec![0u8; blocksize as usize];

        let mut f = self.get_file(&filename.clone())?;

        f.seek(SeekFrom::Start((blk.number() * blocksize) as u64))?;
        f.write_all(b.as_slice())?;

        Ok(blk)
    }

    pub fn len(&mut self, filename: &String) -> std::io::Result<i32> {
        let f = self.get_file(filename)?;
        let len = f.metadata()?.len() as i32 / self.blocksize;
        return Ok(len);
    }

    pub fn block_size(&self) -> i32 {
        self.blocksize
    }
    fn get_file(&self, file_name: &str) -> io::Result<File> {
        let mut open_files = self
            .open_files
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to lock files"))?;

        if let Some(file) = open_files.get(file_name) {
            return Ok(file.try_clone()?);
        }

        // 新しいファイルを開く
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(self.db_directory.join(file_name))?;
        open_files.insert(file_name.to_string(), file.try_clone()?);
        Ok(file)
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
        page.set_bytes(0, &vec![1u8; 100]);

        let (mut fm, _dir) = setup();

        // Write
        fm.write(&blk, &page).unwrap();

        // Read
        let mut read_page = Page::new_from_blocksize(104);
        fm.read(&blk, &mut read_page).unwrap();

        assert_eq!(page.contents(), read_page.contents());
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
        let (fm, dir) = setup();
        let filename = "temptest.db".to_string();

        fm.get_file(&filename).unwrap();

        assert!(dir.path().join(&filename).exists());
    }
}
