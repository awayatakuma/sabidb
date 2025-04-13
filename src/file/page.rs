use crate::constants::INTEGER_BYTES;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct Page {
    bb: Arc<Mutex<Vec<u8>>>,
}

impl Page {
    pub fn new_from_blocksize(blocksize: usize) -> Self {
        return Page {
            bb: Arc::new(Mutex::new(vec![0; blocksize])),
        };
    }

    pub fn new_from_bytes(b: Vec<u8>) -> Self {
        return Page {
            bb: Arc::new(Mutex::new(b)),
        };
    }

    pub fn get_int(&self, offset: usize) -> Result<i32, String> {
        let arr: [u8; 4] = self
            .bb
            .lock()
            .map_err(|_| "failed to get lock")?
            .get(offset..offset + INTEGER_BYTES as usize)
            .ok_or("failed to access a buffer".to_string())?
            .try_into()
            .map_err(|_| "failed to convert slice")?;
        Ok(i32::from_be_bytes(
            arr.try_into()
                .map_err(|_| "failed to convert slice into i32")?,
        ))
    }

    pub fn set_int(&mut self, offset: usize, n: i32) -> Result<(), String> {
        let n_bytes = n.to_be_bytes();
        self.bb.lock().map_err(|_| "failed to get lock")?[offset..offset + INTEGER_BYTES as usize]
            .copy_from_slice(&n_bytes);
        Ok(())
    }

    pub fn get_bytes(&self, offset: usize) -> Result<Vec<u8>, String> {
        let length = self.get_int(offset).unwrap() as usize;
        return Ok(self.bb.lock().map_err(|_| "failed to get lock")?
            [offset + INTEGER_BYTES as usize..offset + INTEGER_BYTES as usize + length]
            .to_vec());
    }

    pub fn set_bytes(&mut self, offset: usize, b: &Vec<u8>) -> Result<(), String> {
        self.set_int(offset, b.len() as i32)?;
        let mut bb = self.bb.lock().map_err(|_| "failed to get lock")?;
        for i in 0..b.len() {
            bb[offset + INTEGER_BYTES as usize + i] = b[i];
        }
        Ok(())
    }

    pub fn get_string(&self, offset: usize) -> Result<String, String> {
        let ret: Vec<u8> = self.get_bytes(offset)?;
        String::from_utf8(ret).map_err(|_| "failed to convert slice into String".to_string())
    }

    pub fn set_string(&mut self, offset: usize, s: &String) -> Result<(), String> {
        let bytes = s.clone().into_bytes();
        self.set_bytes(offset, &bytes)?;
        Ok(())
    }

    pub fn max_length(strlen: usize) -> usize {
        // In this Database, only ascii is allowed to use as string
        const BYTES_PER_CHAR: usize = 1;
        INTEGER_BYTES as usize + (strlen * BYTES_PER_CHAR)
    }

    pub fn contents(&self) -> Arc<Mutex<Vec<u8>>> {
        return self.bb.clone();
    }

    pub(super) fn set_contents(&mut self, b: Vec<u8>) {
        self.bb = Arc::new(Mutex::new(b));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_from_blocksize() {
        let page = Page::new_from_blocksize(100);
        assert_eq!(page.bb.lock().unwrap().len(), 100);
        assert!(page.bb.lock().unwrap().iter().all(|&x| x == 0));
    }

    #[test]
    fn test_new_from_bytes() {
        let data = vec![1, 2, 3, 4, 5];
        let page = Page::new_from_bytes(data.clone());
        assert_eq!(*page.bb.lock().unwrap(), data);
    }

    #[test]
    fn test_get_int() {
        let page = Page::new_from_bytes(vec![0, 0, 0, 1]);
        assert_eq!(page.get_int(0), Ok(1));
    }

    #[test]
    fn test_set_int() {
        let mut page = Page::new_from_blocksize(4);
        page.set_int(0, 42).unwrap();
        assert_eq!(page.get_int(0), Ok(42));
    }

    #[test]
    fn test_get_bytes() {
        let mut data = vec![0; 8];
        data[3] = 3;
        data[4] = 10;
        data[5] = 20;
        data[6] = 30;

        let page = Page::new_from_bytes(data);
        let bytes = page.get_bytes(0);
        assert_eq!(bytes, Ok(vec![10, 20, 30]));
    }

    #[test]
    fn test_set_bytes() {
        let mut page = Page::new_from_blocksize(10);
        let data = vec![1, 2, 3];
        page.set_bytes(0, &data).unwrap();
        assert_eq!(page.bb.lock().unwrap()[4..7], vec![1, 2, 3]);
    }

    #[test]
    fn test_get_string() {
        let mut data = vec![0; 10];
        data[3] = 5;
        data[4..9].copy_from_slice(b"Hello");
        let page = Page::new_from_bytes(data);
        let result = page.get_string(0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello");
    }

    #[test]
    fn test_set_string() {
        let mut page = Page::new_from_blocksize(10);
        page.set_string(0, &"Hello".to_string()).unwrap();
        let res = page.get_string(0);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), "Hello")
    }

    #[test]
    fn test_max_length() {
        let len = Page::max_length(5);
        assert_eq!(len, std::mem::size_of::<i32>() + 5);
    }
}
