use std::sync::{Arc, Mutex};

use crate::{
    rdbc::{connection_adapter::ConnectionAdapter, sql_exception::SQLException},
    server::simple_db::SimpleDB,
    tx::transaction::Transaction,
};

use super::embedded_statement::EmbeddedStatement;

pub struct EmbeddedConnection {
    pub(crate) db: SimpleDB,
    current_tx: Arc<Mutex<Transaction>>,
}

impl EmbeddedConnection {
    pub fn new(db: SimpleDB) -> Self {
        let current_tx = db.new_tx();
        EmbeddedConnection { db, current_tx }
    }

    pub fn commit(&mut self) -> Result<(), crate::rdbc::sql_exception::SQLException> {
        self.current_tx
            .lock()
            .map_err(|e| SQLException::new(e.to_string()))?
            .commit()
            .map_err(|e| SQLException::new(e.to_string()))?;
        self.current_tx = self.db.new_tx();
        Ok(())
    }

    pub(crate) fn get_transaction(&self) -> Arc<Mutex<Transaction>> {
        self.current_tx.clone()
    }

    pub(crate) fn _rollback(&mut self) -> Result<(), crate::rdbc::sql_exception::SQLException> {
        self.current_tx
            .lock()
            .map_err(|e| SQLException::new(e.to_string()))?
            .rollback()
            .map_err(|e| SQLException::new(e.to_string()))?;
        self.current_tx = self.db.new_tx();

        Ok(())
    }
}

impl<'a> ConnectionAdapter<'a> for EmbeddedConnection {
    type Statement = EmbeddedStatement<'a>;

    fn create_statement(
        &'a mut self,
    ) -> Result<EmbeddedStatement<'a>, crate::rdbc::sql_exception::SQLException> {
        Ok(EmbeddedStatement::new(self))
    }

    fn close(&mut self) -> Result<(), crate::rdbc::sql_exception::SQLException> {
        self.commit()
    }
}
