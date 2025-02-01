#[derive(Debug, Clone)]
pub struct BlockId {
    file_name: String,
    blknum: i32,
}

impl PartialEq for BlockId {
    fn eq(&self, other: &Self) -> bool {
        self.file_name == other.file_name && self.blknum == other.blknum
    }
}

impl BlockId {
    pub fn new(file_name: String, blknum: i32) -> Self {
        Self { file_name, blknum }
    }

    pub fn file_name(&self) -> String {
        self.file_name.clone()
    }

    pub fn number(&self) -> i32 {
        self.blknum
    }
}
