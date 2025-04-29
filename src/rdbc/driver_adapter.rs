pub trait DriverAdapter {
    type Con;
    fn connect(dbname: &str) -> Self::Con;
    fn get_major_version() -> i32;
    fn get_minor_version() -> i32;
}
