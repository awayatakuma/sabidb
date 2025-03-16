use std::sync::{Arc, Mutex};

use crate::record::schema::Schema;

use super::{constant::Constant, scan::Scan};

#[derive(Debug, Clone, PartialEq)]
pub struct Expression {
    val: Option<Constant>,
    fldname: Option<String>,
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let content = if let Some(ref val) = self.val {
            val.to_string()
        } else {
            self.fldname.clone().unwrap()
        };
        write!(f, "{}", content)
    }
}

impl Expression {
    pub fn new_from_val(val: Constant) -> Self {
        Expression {
            val: Some(val),
            fldname: None,
        }
    }
    pub fn new_from_fldname(fldname: String) -> Self {
        Expression {
            val: None,
            fldname: Some(fldname),
        }
    }

    pub fn evaluate(&self, s: &dyn Scan) -> Result<Constant, String> {
        let ret = if let Some(val) = &self.val {
            val
        } else {
            &s.get_val(&self.fldname.as_ref().unwrap())?
        };

        Ok(ret.clone())
    }

    pub fn is_field_name(&self) -> bool {
        self.fldname.is_some()
    }

    pub fn as_constant(&self) -> Option<Constant> {
        self.val.clone()
    }

    pub fn as_field_name(&self) -> Option<String> {
        self.fldname.clone()
    }

    pub fn applies_to(&self, sch: Arc<Mutex<Schema>>) -> Result<bool, String> {
        if self.val.is_some() {
            return Ok(true);
        }
        sch.lock()
            .map_err(|_| "failed to get lock")?
            .has_field(&self.fldname.clone().unwrap())
    }
}
