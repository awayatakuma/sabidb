use crate::query::constant::Constant;

#[derive(Debug, Clone)]
pub struct InsertData {
    tblname: String,
    flds: Vec<String>,
    vals: Vec<Constant>,
}

impl InsertData {
    pub fn new(tblname: String, flds: Vec<String>, vals: Vec<Constant>) -> Self {
        InsertData {
            tblname: tblname,
            flds: flds,
            vals: vals,
        }
    }
    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn fields(&self) -> Vec<String> {
        self.flds.clone()
    }

    pub fn vals(&self) -> Vec<Constant> {
        self.vals.clone()
    }
}
