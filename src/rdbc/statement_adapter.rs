use super::sql_exception::SQLException;

pub trait StatementAdapter<'a> {
    type ResultSet;

    fn execute_query(&'a mut self, sql: String) -> Result<Self::ResultSet, SQLException>;
    fn execute_update(&'a mut self, sql: String) -> Result<i32, SQLException>;
}
