use std::path::Path;

use crate::{rdbc::driver_adapter::DriverAdapter, server::simple_db::SimpleDB};

use super::embedded_connection::EmbeddedConnection;

pub struct EmbeddedDriver {}

impl DriverAdapter for EmbeddedDriver {
    type Con = EmbeddedConnection;

    fn connect(dbpath: &Path) -> Self::Con {
        let db = SimpleDB::new_with_refined_planners(dbpath);
        EmbeddedConnection::new(db)
    }

    fn get_major_version() -> i32 {
        0
    }

    fn get_minor_version() -> i32 {
        1
    }
}
