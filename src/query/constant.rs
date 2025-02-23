#[derive(Debug, Clone)]
pub struct Constant {
    ival: Option<i32>,
    sval: Option<String>,
}

impl std::cmp::PartialEq for Constant {
    fn eq(&self, other: &Self) -> bool {
        if self.ival.is_some() {
            self.ival == other.ival
        } else if self.sval.is_some() {
            self.sval == self.sval
        } else {
            panic!("unreachable!!")
        }
    }
}

impl std::cmp::Eq for Constant {}

impl std::cmp::PartialOrd for Constant {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.ival.partial_cmp(&other.ival) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.sval.partial_cmp(&other.sval)
    }
}

impl std::hash::Hash for Constant {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        if self.ival.is_some() {
            self.ival.hash(state);
        } else if self.sval.is_some() {
            self.sval.hash(state);
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
        } else {
            panic!("unreachable!!")
        };
        write!(f, "{}", val)
    }
}

impl Constant {
    pub fn mew_from_i32(ival: i32) -> Self {
        Constant {
            ival: Some(ival),
            sval: None,
        }
    }
    pub fn mew_from_string(sval: String) -> Self {
        Constant {
            ival: None,
            sval: Some(sval),
        }
    }

    pub fn as_int(&self) -> Option<i32> {
        self.ival.clone()
    }
    pub fn as_string(&self) -> Option<String> {
        self.sval.clone()
    }
}
