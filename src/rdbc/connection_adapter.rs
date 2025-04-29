use super::{sql_exception::SQLException, statement_adapter::StatementAdapter};

pub trait ConnectionAdapter<'a> {
    type Statement: StatementAdapter<'a>;
    fn create_statement(&'a mut self) -> Result<Self::Statement, SQLException>;
    fn close(&mut self) -> Result<(), SQLException>;
}
