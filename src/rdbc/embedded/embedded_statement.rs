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
        qry: &String,
    ) -> Result<EmbeddedResultSet<'a>, crate::rdbc::sql_exception::SQLException> {
        let tx = self.conn.get_transaction();
        let pln = self
            .conn
            .db
            .planner
            .as_mut()
            .unwrap()
            .create_query_planner(qry, tx)
            .map_err(|e| SQLException::new(e.to_string()))?;
        Ok(EmbeddedResultSet::new(pln, self.conn)?)
    }

    fn execute_update(
        &mut self,
        cmd: &String,
    ) -> Result<i32, crate::rdbc::sql_exception::SQLException> {
        let tx = self.conn.get_transaction();
        let result = self
            .conn
            .db
            .planner
            .as_mut()
            .unwrap()
            .execute_update(&cmd, tx)
            .map_err(|e| SQLException::new(e.to_string()))?;
        self.conn.commit()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::rdbc::connection_adapter::ConnectionAdapter;
    use crate::rdbc::driver_adapter::DriverAdapter;
    use crate::rdbc::embedded::embedded_driver::EmbeddedDriver;
    use crate::rdbc::statement_adapter::StatementAdapter;
    use tempfile::TempDir;

    #[test]
    fn test_exception_rdbc_propagation() {
        let temp_dir = TempDir::new().unwrap();
        let mut conn = EmbeddedDriver::connect(temp_dir.path());
        let mut stmt = conn.create_statement().unwrap();

        // Try to query a non-existent table
        let res = stmt.execute_query(&"select a from non_existent".to_string());
        assert!(res.is_err());

        if let Err(err) = res {
            // Let's see what the actual error message is
            let msg = format!("{}", err);
            assert!(msg.contains("SQL Exception"));
            // The actual error from MetadataManager/TableManager is probably "table non_existent not found"
            assert!(msg.contains("non_existent"));
        } else {
            panic!("Should have been an error");
        }
    }
}
