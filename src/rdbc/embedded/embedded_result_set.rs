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
            .map_err(|_| SQLException {})?
            .open()
            .map_err(|_| SQLException {})?;
        let sch = Arc::new(Mutex::new(
            plan.lock()
                .map_err(|_| SQLException {})?
                .schema()
                .map_err(|_| SQLException {})?,
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
            .map_err(|_| SQLException {})?
            .before_first()
            .map_err(|_| SQLException {})
    }

    fn next(&self) -> Result<bool, crate::rdbc::sql_exception::SQLException> {
        self.s
            .lock()
            .map_err(|_| SQLException {})?
            .next()
            .map_err(|_| SQLException {})
    }

    fn get_int(&self, fldname: String) -> Result<i32, SQLException> {
        let fldname = fldname.to_lowercase();
        self.s
            .lock()
            .map_err(|_| SQLException {})?
            .get_int(&fldname)
            .map_err(|_| SQLException {})
    }

    fn get_string(&self, fldname: String) -> Result<String, SQLException> {
        let fldname = fldname.to_lowercase();
        self.s
            .lock()
            .map_err(|_| SQLException {})?
            .get_string(&fldname)
            .map_err(|_| SQLException {})
    }

    fn get_metadata(&self) -> Result<Self::ResultSetMetadata, SQLException> {
        Ok(EmbeddedMetadata::new(self.sch.clone()))
    }

    fn close(&mut self) -> Result<(), SQLException> {
        self.s
            .lock()
            .map_err(|_| SQLException {})?
            .close()
            .map_err(|_| SQLException {})?;
        self.conn.commit()?;

        Ok(())
    }
}
