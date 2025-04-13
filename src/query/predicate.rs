use std::{
    fmt,
    sync::{Arc, Mutex},
};

use crate::{plan::plan::Plan, record::schema::Schema};

use super::{constant::Constant, scan::Scan, term::Term};

#[derive(Debug, Clone, PartialEq)]
pub struct Predicate {
    terms: Vec<Term>,
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let content = self
            .terms
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<String>>()
            .join(" and ");

        write!(f, "{}", content)
    }
}

impl Predicate {
    pub fn new() -> Self {
        Predicate { terms: Vec::new() }
    }

    pub fn new_from_term(t: Term) -> Self {
        let terms = vec![t];
        Predicate { terms }
    }

    pub fn conjoin_with(&mut self, pred: &Predicate) {
        self.terms.extend(pred.terms.iter().cloned());
    }

    pub fn is_satisfied(&self, s: Arc<Mutex<dyn Scan>>) -> Result<bool, String> {
        for term in self.terms.iter() {
            if !term.is_satisfied(s.clone())? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn reduction_factor(&self, p: Arc<Mutex<dyn Plan>>) -> i32 {
        let factor = self.terms.iter().fold(1, |factor, t| {
            factor * t.reduction_factor(p.clone()).unwrap()
        });

        factor
    }

    pub fn select_sub_pred(&self, sch: Arc<Mutex<Schema>>) -> Result<Option<Predicate>, String> {
        let mut result = Predicate::new();
        for t in &self.terms {
            if t.applies_to(sch.clone())? {
                // Todo check if t can be cloned
                result.terms.push(t.clone());
            }
        }
        if result.terms.len() == 0 {
            return Ok(None);
        }

        Ok(Some(result))
    }

    pub fn join_sub_pred(
        &self,
        sch1: Arc<Mutex<Schema>>,
        sch2: Arc<Mutex<Schema>>,
    ) -> Result<Option<Predicate>, String> {
        let mut result = Predicate::new();
        let mut newsch = Schema::new();
        newsch.add_all(sch1.clone())?;
        newsch.add_all(sch2.clone())?;
        let newsch = Arc::new(Mutex::new(newsch));
        for t in &self.terms {
            if !t.applies_to(sch1.clone())?
                && !t.applies_to(sch2.clone())?
                && t.applies_to(newsch.clone())?
            {
                // Todo check if t can be cloned
                result.terms.push(t.clone());
            }
        }

        if result.terms.len() == 0 {
            return Ok(None);
        }

        Ok(Some(result))
    }

    pub fn equate_with_constant(&self, fldname: String) -> Option<Constant> {
        for t in &self.terms {
            if let Some(c) = t.equate_with_constant(fldname.clone()) {
                return Some(c);
            }
        }

        None
    }

    pub fn equate_with_field(&self, fldname: String) -> Option<String> {
        for t in &self.terms {
            if let Some(c) = t.equate_with_field(fldname.clone()) {
                return Some(c);
            }
        }

        None
    }
}
