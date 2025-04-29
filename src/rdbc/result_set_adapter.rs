use super::sql_exception::SQLException;

pub trait ResultSetAdapter {
    type ResultSetMetadata;

    fn next(&self) -> Result<bool, SQLException>;
    fn get_int(&self, fldname: String) -> Result<i32, SQLException>;
    fn get_string(&self, fldname: String) -> Result<String, SQLException>;
    fn get_metadata(&self) -> Result<Self::ResultSetMetadata, SQLException>;
    fn close(&mut self) -> Result<(), SQLException>;
}
