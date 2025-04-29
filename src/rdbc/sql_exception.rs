use core::fmt;

#[derive(Debug)]
pub struct SQLException {}

impl std::error::Error for SQLException {}
impl fmt::Display for SQLException {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "runtime error")
    }
}
