use std::clone;

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct BlockId {
    file_name: String,
    blknum: i32,
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
