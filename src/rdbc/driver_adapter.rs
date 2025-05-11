use std::path::Path;

pub trait DriverAdapter {
    type Con;
    fn connect(dbname: &Path) -> Self::Con;
    fn get_major_version() -> i32;
    fn get_minor_version() -> i32;
}
