use std::sync::{Arc, Mutex};

use crate::{
    plan::plan::Plan,
    query::scan::Scan,
    rdbc::{result_set_adapter::ResultSetAdapter, sql_exception::SQLException},
    record::schema::Schema,
};

use super::{embedded_connection::EmbeddedConnection, embedded_metadata::EmbeddedMetadata};

pub struct EmbeddedResultSet<'a> {
    s: Arc<Mutex<dyn Scan>>,
    sch: Arc<Mutex<Schema>>,
    conn: &'a mut EmbeddedConnection,
}

impl<'a> EmbeddedResultSet<'a> {
    pub fn new(
        plan: Arc<Mutex<dyn Plan>>,
        conn: &'a mut EmbeddedConnection,
    ) -> Result<Self, SQLException> {
        let s = plan
            .lock()
            .map_err(|e| SQLException::new(e.to_string()))?
            .open()
            .map_err(|e| SQLException::new(e.to_string()))?;
        let sch = Arc::new(Mutex::new(
            plan.lock()
                .map_err(|e| SQLException::new(e.to_string()))?
                .schema()
                .map_err(|e| SQLException::new(e.to_string()))?,
        ));
        Ok(EmbeddedResultSet {
            s: s,
            sch: sch,
            conn,
        })
    }
}

impl<'a> ResultSetAdapter for EmbeddedResultSet<'a> {
    type ResultSetMetadata = EmbeddedMetadata;

    fn before_first(&self) -> Result<(), crate::rdbc::sql_exception::SQLException> {
        self.s
            .lock()
            .map_err(|e| SQLException::new(e.to_string()))?
            .before_first()
            .map_err(|e| SQLException::new(e.to_string()))
    }

    fn next(&self) -> Result<bool, crate::rdbc::sql_exception::SQLException> {
        self.s
            .lock()
            .map_err(|e| SQLException::new(e.to_string()))?
            .next()
            .map_err(|e| SQLException::new(e.to_string()))
    }

    fn get_int(&self, fldname: String) -> Result<i32, SQLException> {
        let fldname = fldname.to_lowercase();
        self.s
            .lock()
            .map_err(|e| SQLException::new(e.to_string()))?
            .get_int(&fldname)
            .map_err(|e| SQLException::new(e.to_string()))
    }

    fn get_string(&self, fldname: String) -> Result<String, SQLException> {
        let fldname = fldname.to_lowercase();
        self.s
            .lock()
            .map_err(|e| SQLException::new(e.to_string()))?
            .get_string(&fldname)
            .map_err(|e| SQLException::new(e.to_string()))
    }

    fn get_metadata(&self) -> Result<Self::ResultSetMetadata, SQLException> {
        Ok(EmbeddedMetadata::new(self.sch.clone()))
    }

    fn close(&mut self) -> Result<(), SQLException> {
        self.s
            .lock()
            .map_err(|e| SQLException::new(e.to_string()))?
            .close()
            .map_err(|e| SQLException::new(e.to_string()))?;
        self.conn.commit()?;

        Ok(())
    }
}
