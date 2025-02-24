use crate::{query::constant::Constant, record::rid::RID};

pub(crate) trait Index {
    fn before_first(&mut self, search_key: Constant) -> Result<(), String>;
    fn next(&self) -> Result<bool, String>;
    fn get_data_rid(&self) -> Result<RID, String>;
    fn insert(&mut self, dataval: Constant, datarid: RID) -> Result<(), String>;
    fn delete(&mut self, dataval: Constant, datarid: RID) -> Result<(), String>;
    fn close(&mut self) -> Result<(), String>;
}
