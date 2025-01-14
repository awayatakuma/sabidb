
#[derive(PartialEq, Eq, Debug)]
pub struct BlockId {
    file_name: String,
    blknum: u64,
}

impl BlockId {
    pub fn new(file_name: String, blknum: u64) -> Self {
        Self { file_name, blknum }
    }

    pub fn file_name(&self) -> String {
        self.file_name.clone()
    }

    pub fn number(&self) -> u64 {
        self.blknum
    }
}
