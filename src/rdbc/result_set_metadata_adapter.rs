use super::sql_exception::SQLException;

pub trait ResultSetMetadataAdapter {
    fn get_column_count(&self) -> Result<i32, SQLException>;
    fn get_column_name(&self, column: i32) -> Result<Option<String>, SQLException>;
    fn get_column_type(&self, column: i32) -> Result<Option<i32>, SQLException>;
    fn get_column_display_size(&self, column: i32) -> Result<i32, SQLException>;
}
