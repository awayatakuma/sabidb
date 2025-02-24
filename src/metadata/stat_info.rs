#[derive(Debug, Clone)]
pub struct StatInfo {
    num_blocks: i32,
    num_recs: i32,
}

impl StatInfo {
    pub fn new(num_blocks: i32, num_recs: i32) -> Self {
        StatInfo {
            num_blocks: num_blocks,
            num_recs: num_recs,
        }
    }

    pub fn blocks_accessed(&self) -> i32 {
        self.num_blocks
    }

    pub fn records_output(&self) -> i32 {
        self.num_recs
    }

    pub fn distinct_values(&self, _fldname: String) -> i32 {
        1 + (self.num_recs / 3)
    }
}
