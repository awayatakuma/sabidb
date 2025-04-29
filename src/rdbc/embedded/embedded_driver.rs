use std::path::Path;

use crate::{rdbc::driver_adapter::DriverAdapter, server::simple_db::SimpleDB};

use super::embedded_connection::EmbeddedConnection;

pub struct EmbeddedAdapter {}

impl DriverAdapter for EmbeddedAdapter {
    type Con = EmbeddedConnection;

    fn connect(dbname: &str) -> Self::Con {
        let path = Path::new(dbname);
        let db = SimpleDB::new(path);
        EmbeddedConnection::new(db)
    }

    fn get_major_version() -> i32 {
        0
    }

    fn get_minor_version() -> i32 {
        1
    }
}
