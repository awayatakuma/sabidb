use std::collections::HashMap;
use std::fs::{create_dir, remove_file, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::file::block_id::BlockId;
use crate::file::page::Page;

pub struct FileManager {
    blocksize: i32,
    db_directory: PathBuf,
    open_files: HashMap<String, File>,
}

impl FileManager {
    pub fn new_from_blocksize(db_directory: &Path, blocksize: i32) -> Self {
        // TODO: need to implement to create directory and remove temp files
        if !db_directory.exists() {
            let _ = create_dir(db_directory);
        }

        for file in db_directory.iter() {
            if file.to_string_lossy().starts_with("temp") {
                let _ = remove_file(file);
            }
        }

        Self {
            blocksize,
            db_directory: db_directory.to_path_buf(),
            open_files: HashMap::new(),
        }
    }

    pub fn read(&mut self, blk: &BlockId, p: &mut Page) -> std::io::Result<()> {
        let blocksize = self.blocksize;
        let contents = p.contents();

        let f = self.get_file(&blk.file_name())?;
        f.seek(SeekFrom::Start((blk.number() * blocksize) as u64))?;
        let _ = f.read(contents.borrow_mut().as_mut_slice());
        p.set_contents(contents.borrow().clone());
        Ok(())
    }

    pub fn write(&mut self, blk: &BlockId, p: &Page) -> std::io::Result<()> {
        let blocksize = self.blocksize;
        let f = self.get_file(&blk.file_name())?;
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

        let f = self.get_file(&filename.clone())?;

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

    fn get_file(&mut self, filename: &String) -> io::Result<&mut File> {
        if !self.open_files.contains_key(filename) {
            let db_table = Path::new(&self.db_directory).join(&filename);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(db_table)?;

            self.open_files.insert(filename.to_string(), file);
        }

        Ok(self.open_files.get_mut(filename).unwrap())
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
        let (mut fm, _temp_dir) = setup();
        let filename = "test.db".to_string();
        let blk = BlockId::new(filename.clone(), 0);
        let mut page = Page::new_from_blocksize(104);
        page.set_bytes(0, &vec![1u8; 100]);

        // Write
        fm.write(&blk, &page).unwrap();

        // Read
        let mut read_page = Page::new_from_blocksize(104);
        fm.read(&blk, &mut read_page).unwrap();

        assert_eq!(page.contents(), read_page.contents());
    }

    #[test]
    fn test_append() {
        let (mut fm, _temp_dir) = setup();
        let filename = "test.db".to_string();

        fm.append(&filename).unwrap();
        assert_eq!(fm.len(&filename).unwrap(), 1);

        fm.append(&filename).unwrap();
        assert_eq!(fm.len(&filename).unwrap(), 2);
    }

    #[test]
    fn test_len() {
        let (mut fm, _temp_dir) = setup();
        let filename = "test.db".to_string();

        assert_eq!(fm.len(&filename).unwrap(), 0);

        fm.append(&filename).unwrap();
        assert_eq!(fm.len(&filename).unwrap(), 1);
    }

    #[test]
    fn test_get_file() {
        let (mut fm, temp_dir) = setup();
        let filename = "test.db".to_string();

        fm.get_file(&filename).unwrap();

        assert!(temp_dir.path().join(&filename).exists());
    }
}
