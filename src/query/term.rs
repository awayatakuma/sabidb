use std::{
    i32,
    sync::{Arc, Mutex},
};

use crate::{plan::plan::Plan, record::schema::Schema};

use super::{constant::Constant, expression::Expression, scan::Scan};

#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    Equate(Expression, Expression),
    In(Expression, Vec<Constant>),
}

impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Term::Equate(lhs, rhs) => write!(f, "{} = {}", lhs.to_string(), rhs.to_string()),
            Term::In(lhs, rhs_list) => {
                let list = rhs_list
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "{} in ({})", lhs.to_string(), list)
            }
        }
    }
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Term::Equate(lhs, rhs)
    }

    pub fn new_in(lhs: Expression, rhs_list: Vec<Constant>) -> Self {
        Term::In(lhs, rhs_list)
    }

    pub fn is_satisfied(&self, s: Arc<Mutex<dyn Scan>>) -> Result<bool, String> {
        match self {
            Term::Equate(lhs, rhs) => {
                let lhsval = lhs.evaluate(s.clone())?;
                let rhsval = rhs.evaluate(s)?;
                Ok(rhsval.eq(&lhsval))
            }
            Term::In(lhs, rhs_list) => {
                let lhsval = lhs.evaluate(s)?;
                for val in rhs_list {
                    if val.eq(&lhsval) {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    pub fn reduction_factor(&self, p: Arc<Mutex<dyn Plan>>) -> Result<i32, String> {
        match self {
            Term::Equate(lhs, rhs) => {
                if let (Some(lhs_name), Some(rhs_name)) = (lhs.as_field_name(), rhs.as_field_name()) {
                    let locked_p = p.lock().map_err(|_| "failed to get lock")?;
                    return Ok(i32::max(
                        locked_p.distinct_values(lhs_name)?,
                        locked_p.distinct_values(rhs_name)?,
                    ));
                } else if let Some(lhs_name) = lhs.as_field_name() {
                    return p
                        .lock()
                        .map_err(|_| "failed to get lock")?
                        .distinct_values(lhs_name);
                } else if let Some(rhs_name) = rhs.as_field_name() {
                    return p
                        .lock()
                        .map_err(|_| "failed to get lock")?
                        .distinct_values(rhs_name);
                } else if lhs.as_constant().eq(&rhs.as_constant()) {
                    return Ok(1);
                }
                Ok(i32::MAX)
            }
            Term::In(lhs, rhs_list) => {
                if let Some(lhs_name) = lhs.as_field_name() {
                    let distinct = p
                        .lock()
                        .map_err(|_| "failed to get lock")?
                        .distinct_values(lhs_name)?;
                    // Simple estimation: reduction factor is roughly distinct_values / list_size
                    Ok(i32::max(1, distinct / rhs_list.len() as i32))
                } else {
                    Ok(i32::MAX)
                }
            }
        }
    }

    pub fn equate_with_constant(&self, fldname: &String) -> Option<Constant> {
        match self {
            Term::Equate(lhs, rhs) => {
                if lhs.is_field_name()
                    && lhs.as_field_name().eq(&Some(fldname.clone()))
                    && !rhs.is_field_name()
                {
                    return rhs.as_constant();
                } else if rhs.is_field_name()
                    && rhs.as_field_name().eq(&Some(fldname.clone()))
                    && !lhs.is_field_name()
                {
                    return lhs.as_constant();
                }
                None
            }
            Term::In(_, _) => None, // IN doesn't equate to a single constant
        }
    }

    pub fn equate_with_field(&self, fldname: &String) -> Option<String> {
        match self {
            Term::Equate(lhs, rhs) => {
                if lhs.is_field_name()
                    && lhs.as_field_name().eq(&Some(fldname.clone()))
                    && rhs.is_field_name()
                {
                    return rhs.as_field_name();
                } else if rhs.is_field_name()
                    && rhs.as_field_name().eq(&Some(fldname.clone()))
                    && lhs.is_field_name()
                {
                    return lhs.as_field_name();
                }
                None
            }
            Term::In(_, _) => None,
        }
    }

    pub fn applies_to(&self, sch: &Schema) -> Result<bool, String> {
        match self {
            Term::Equate(lhs, rhs) => Ok(lhs.applies_to(sch)? && rhs.applies_to(sch)?),
            Term::In(lhs, _) => lhs.applies_to(sch),
        }
    }
}
