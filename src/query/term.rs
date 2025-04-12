use std::{
    i32,
    sync::{Arc, Mutex},
};

use crate::{plan::plan::Plan, record::schema::Schema};

use super::{constant::Constant, expression::Expression, scan::Scan};

#[derive(Debug, Clone, PartialEq)]
pub struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.lhs.to_string(), self.rhs.to_string())
    }
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Term { lhs: lhs, rhs: rhs }
    }

    pub fn is_satisfied(&self, s: &Box<dyn Scan>) -> Result<bool, String> {
        let lhsval = self.lhs.evaluate(s)?;
        let rhsval = self.rhs.evaluate(s)?;
        Ok(rhsval.eq(&lhsval))
    }

    pub fn reduction_factor(&self, p: &Box<dyn Plan>) -> Result<i32, String> {
        if let (Some(lhs_name), Some(rhs_name)) =
            (self.lhs.as_field_name(), self.rhs.as_field_name())
        {
            return Ok(i32::max(
                p.distinct_values(lhs_name)?,
                p.distinct_values(rhs_name)?,
            ));
        } else if let Some(lhs_name) = self.lhs.as_field_name() {
            return p.distinct_values(lhs_name);
        } else if let Some(rhs_name) = self.rhs.as_field_name() {
            return p.distinct_values(rhs_name);
        } else if self.lhs.as_constant().eq(&self.rhs.as_constant()) {
            return Ok(1);
        }
        Ok(i32::MAX)
    }

    pub fn equate_with_constant(&self, fldname: String) -> Option<Constant> {
        if self.lhs.is_field_name()
            && self.lhs.as_field_name().eq(&Some(fldname.clone()))
            && !self.rhs.is_field_name()
        {
            return self.rhs.as_constant();
        } else if self.rhs.is_field_name()
            && self.rhs.as_field_name().eq(&Some(fldname))
            && !self.lhs.is_field_name()
        {
            return self.lhs.as_constant();
        }

        None
    }

    pub fn equate_with_field(&self, fldname: String) -> Option<String> {
        if self.lhs.is_field_name()
            && self.lhs.as_field_name().eq(&Some(fldname.clone()))
            && !self.rhs.is_field_name()
        {
            return self.rhs.as_field_name();
        } else if self.rhs.is_field_name()
            && self.rhs.as_field_name().eq(&Some(fldname))
            && !self.lhs.is_field_name()
        {
            return self.lhs.as_field_name();
        }

        None
    }

    pub fn applies_to(&self, sch: Arc<Mutex<Schema>>) -> Result<bool, String> {
        Ok(self.lhs.applies_to(sch.clone())? && self.rhs.applies_to(sch.clone())?)
    }
}
