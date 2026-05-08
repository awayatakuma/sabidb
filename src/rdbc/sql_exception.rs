use core::fmt;

#[derive(Debug, PartialEq, Clone)]
pub struct SQLException {
    message: String,
}

impl SQLException {
    pub fn new(message: String) -> Self {
        SQLException { message }
    }
}

impl std::error::Error for SQLException {}

impl fmt::Display for SQLException {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SQL Exception: {}", self.message)
    }
}
