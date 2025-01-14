pub struct Page {
    bb: Vec<u8>,
}

impl Page {
    pub fn new_from_blocksize(blocksize: usize) -> Self {
        return Page {
            bb: vec![0; blocksize],
        };
    }

    pub fn new_from_bytes(b: Vec<u8>) -> Self {
        return Page { bb: b };
    }

    pub fn get_int(&self, offset: usize) -> Option<u8> {
        let ret = self.bb.get(offset).copied();
        ret
    }

    pub fn set_int(&mut self, offset: usize, n: u8) {
        self.bb[offset] = n
    }

    pub fn get_bytes(&self, offset: usize) -> Vec<u8> {
        let length = self.bb[offset] as usize;
        return self.bb[offset + 1..=offset + length].to_vec();
    }

    pub fn set_bytes(&mut self, offset: usize, b: &Vec<u8>) {
        self.bb[offset] = b.len() as u8;
        for i in 0..b.len() {
            self.bb[offset + 1 + i] = b[i];
        }
    }

    pub fn get_string(&self, offset: usize) -> Result<String, std::string::FromUtf8Error> {
        let ret: Vec<u8> = self.get_bytes(offset);
        return String::from_utf8(ret);
    }

    pub fn set_string(&mut self, offset: usize, s: &String) {
        let bytes = s.clone().into_bytes();
        self.set_bytes(offset, &bytes);
    }

    pub fn max_length(strlen: usize) -> usize {
        // In this Database, only ascii is allowed to use as string
        const BYTES_PER_CHAR: usize = 1;
        std::mem::size_of::<i32>() + (strlen * BYTES_PER_CHAR)
    }

    pub(super) fn contents(&self) -> Vec<u8> {
        return self.bb.clone();
    }

    pub(super) fn set_contents(&mut self, b: Vec<u8>) {
        self.bb = b;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_from_blocksize() {
        let page = Page::new_from_blocksize(100);
        assert_eq!(page.bb.len(), 100);
        assert!(page.bb.iter().all(|&x| x == 0));
    }

    #[test]
    fn test_new_from_bytes() {
        let data = vec![1, 2, 3, 4, 5];
        let page = Page::new_from_bytes(data.clone());
        assert_eq!(page.bb, data);
    }

    #[test]
    fn test_get_int() {
        let page = Page::new_from_bytes(vec![10, 20, 30]);
        assert_eq!(page.get_int(0), Some(10));
        assert_eq!(page.get_int(1), Some(20));
        assert_eq!(page.get_int(3), None); // Out of bounds
    }

    #[test]
    fn test_set_int() {
        let mut page = Page::new_from_blocksize(3);
        page.set_int(0, 42);
        assert_eq!(page.get_int(0), Some(42));
    }

    #[test]
    fn test_get_bytes() {
        let mut data = vec![0; 10];
        data[3] = 3; // Length of subsequent data
        data[4] = 10;
        data[5] = 20;
        data[6] = 30;
        let page = Page::new_from_bytes(data);
        let bytes = page.get_bytes(3);
        assert_eq!(bytes, vec![10, 20, 30]);
    }

    #[test]
    fn test_set_bytes() {
        let mut page = Page::new_from_blocksize(10);
        let data = vec![1, 2, 3];
        page.set_bytes(0, &data);
        assert_eq!(page.bb[1..4], vec![1, 2, 3]);
    }

    #[test]
    fn test_get_string() {
        let mut data = vec![0; 10];
        data[0] = 5;
        data[1..6].copy_from_slice(b"Hello");
        let page = Page::new_from_bytes(data);
        let result = page.get_string(0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Hello");
    }

    #[test]
    fn test_set_string() {
        let mut page = Page::new_from_blocksize(10);
        page.set_string(0, &"Hello".to_string());
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
