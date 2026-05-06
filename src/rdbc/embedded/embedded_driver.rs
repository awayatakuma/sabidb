use std::path::Path;

use crate::{rdbc::driver_adapter::DriverAdapter, server::simple_db::SimpleDB};

use super::embedded_connection::EmbeddedConnection;

pub struct EmbeddedDriver {}

impl DriverAdapter for EmbeddedDriver {
    type Con = EmbeddedConnection;

    fn connect(dbpath: &Path) -> Self::Con {
        let db = SimpleDB::new(dbpath);
        // if you try HeuristicQueryPlanner, please comment out the above row and use the below row instead.
        // Please be careful HeuristicQueryPlanner is not incomplete and it does not perform some funcitons like views.
        // let db = SimpleDB::new_with_refined_planners(dbpath);
        EmbeddedConnection::new(db)
    }

    fn get_major_version() -> i32 {
        0
    }

    fn get_minor_version() -> i32 {
        1
    }
}
