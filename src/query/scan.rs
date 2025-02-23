use super::constant::Constant;

pub trait Scan {
    fn before_first(&mut self) -> Result<(), String>;
    fn next(&mut self) -> Result<bool, String>;
    fn get_int(&self, fldname: &String) -> Result<i32, String>;
    fn get_string(&self, fldname: &String) -> Result<String, String>;
    fn get_val(&self, fldname: &String) -> Result<Constant, String>;
    fn has_field(&self, fldname: &String) -> Result<bool, String>;
    fn close(&mut self) -> Result<(), String>;
}
