use crate::record::rid::RID;

use super::{constant::Constant, scan::Scan};

pub trait UpdateScan: Scan {
    fn set_val(&mut self, fldname: String, val: Constant) -> Result<(), String>;
    fn set_int(&mut self, fldname: String, val: i32) -> Result<(), String>;
    fn set_string(&mut self, fldname: String, val: String) -> Result<(), String>;
    fn insert(&mut self) -> Result<(), String>;
    fn delete(&mut self) -> Result<(), String>;
    fn get_rid(&mut self) -> Result<RID, String>;
    fn move_to_rid(&mut self, rid: RID) -> Result<(), String>;

    fn to_scan(&mut self) -> Result<&dyn Scan, String>;
}
