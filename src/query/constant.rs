use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Debug, Clone)]
pub struct Constant {
    ival: Option<i32>,
    sval: Option<String>,
    bval: Option<bool>,
}

impl std::cmp::PartialEq for Constant {
    fn eq(&self, other: &Self) -> bool {
        if self.ival.is_some() {
            self.ival == other.ival
        } else if self.sval.is_some() {
            self.sval == other.sval
        } else if self.bval.is_some() {
            self.bval == other.bval
        } else {
            panic!("unreachable!!")
        }
    }
}

impl std::cmp::Eq for Constant {}

impl std::cmp::PartialOrd for Constant {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if let (Some(l), Some(r)) = (self.ival, other.ival) {
            return l.partial_cmp(&r);
        }
        if let (Some(l), Some(r)) = (&self.sval, &other.sval) {
            return l.partial_cmp(r);
        }
        if let (Some(l), Some(r)) = (self.bval, other.bval) {
            return l.partial_cmp(&r);
        }
        None
    }
}

impl std::hash::Hash for Constant {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if let Some(val) = self.ival {
            val.hash(state);
        } else if let Some(val) = &self.sval {
            val.hash(state);
        } else if let Some(val) = self.bval {
            val.hash(state);
        } else {
            panic!("unreachable!!")
        }
    }
}

impl std::fmt::Display for Constant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let val = if let Some(val) = self.ival {
            val.to_string()
        } else if let Some(val) = &self.sval {
            val.clone()
        } else if let Some(val) = self.bval {
            val.to_string()
        } else {
            panic!("unreachable!!")
        };
        write!(f, "{}", val)
    }
}

impl Constant {
    pub fn new_from_i32(ival: i32) -> Self {
        Constant {
            ival: Some(ival),
            sval: None,
            bval: None,
        }
    }
    pub fn new_from_string(sval: String) -> Self {
        Constant {
            ival: None,
            sval: Some(sval),
            bval: None,
        }
    }
    pub fn new_from_bool(bval: bool) -> Self {
        Constant {
            ival: None,
            sval: None,
            bval: Some(bval),
        }
    }

    pub fn as_int(&self) -> Option<i32> {
        self.ival.clone()
    }
    pub fn as_string(&self) -> Option<String> {
        self.sval.clone()
    }
    pub fn as_bool(&self) -> Option<bool> {
        self.bval.clone()
    }

    pub fn hash_code(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_constant() {
        let c_true = Constant::new_from_bool(true);
        let c_false = Constant::new_from_bool(false);
        assert_eq!(c_true.as_bool(), Some(true));
        assert_eq!(c_false.as_bool(), Some(false));
        assert_ne!(c_true, c_false);
        assert_eq!(c_true.to_string(), "true");
    }
}
