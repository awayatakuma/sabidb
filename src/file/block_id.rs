#[derive(Debug, Clone)]
pub struct BlockId {
    file_name: String,
    blknum: i32,
}

impl std::cmp::PartialEq for BlockId {
    fn eq(&self, other: &Self) -> bool {
        self.file_name == other.file_name && self.blknum == other.blknum
    }
}

impl std::cmp::Eq for BlockId {}

impl std::fmt::Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[file {}, block {} ]", self.file_name, self.blknum)
    }
}

impl std::hash::Hash for BlockId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.file_name.hash(state);
        self.blknum.hash(state);
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
