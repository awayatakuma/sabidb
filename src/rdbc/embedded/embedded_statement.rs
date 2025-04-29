use crate::rdbc::{sql_exception::SQLException, statement_adapter::StatementAdapter};

use super::{embedded_connection::EmbeddedConnection, embedded_result_set::EmbeddedResultSet};

pub struct EmbeddedStatement<'a> {
    conn: &'a mut EmbeddedConnection,
}

impl<'a> EmbeddedStatement<'a> {
    pub fn new(conn: &'a mut EmbeddedConnection) -> Self {
        EmbeddedStatement { conn }
    }
}

impl<'a> StatementAdapter<'a> for EmbeddedStatement<'a> {
    type ResultSet = EmbeddedResultSet<'a>;

    fn execute_query(
        &'a mut self,
        qry: String,
    ) -> Result<EmbeddedResultSet, crate::rdbc::sql_exception::SQLException> {
        let tx = self.conn.get_transaction();
        let pln = self
            .conn
            .db
            .planner
            .as_mut()
            .unwrap()
            .create_query_planner(qry, tx)
            .map_err(|_| SQLException {})?;
        Ok(EmbeddedResultSet::new(pln, self.conn)?)
    }

    fn execute_update(
        &mut self,
        cmd: String,
    ) -> Result<i32, crate::rdbc::sql_exception::SQLException> {
        let tx = self.conn.get_transaction();
        let result = self
            .conn
            .db
            .planner
            .as_mut()
            .unwrap()
            .execute_update(&cmd, tx)
            .map_err(|_| SQLException {})?;
        self.conn.commit()?;
        Ok(result)
    }
}
