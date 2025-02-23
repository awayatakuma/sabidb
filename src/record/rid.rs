#[derive(Debug, Clone)]
pub struct RID {
    blknum: i32,
    slot: i32,
}

impl std::cmp::PartialEq for RID {
    fn eq(&self, other: &Self) -> bool {
        self.blknum == other.blknum && self.slot == other.slot
    }
}

impl std::cmp::Eq for RID {}

impl std::fmt::Display for RID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[ {},  {} ]", self.blknum, self.slot)
    }
}

impl RID {
    pub fn new(blknum: i32, slot: i32) -> Self {
        RID { blknum, slot }
    }

    pub fn block_number(&self) -> i32 {
        self.blknum
    }

    pub fn slot(&self) -> i32 {
        self.slot
    }
}
