use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::query::{constant::Constant, scan::Scan};

pub struct GroupValue {
    vals: HashMap<String, Constant>,
}

impl GroupValue {
    pub fn new(s: Arc<Mutex<dyn Scan>>, fields: Vec<String>) -> Result<Self, String> {
        let mut vals = HashMap::new();
        for fldname in fields {
            vals.insert(
                fldname.clone(),
                s.lock()
                    .map_err(|_| "failed to get lock")?
                    .get_val(&fldname)?,
            );
        }
        Ok(GroupValue { vals })
    }

    pub fn get_val(&self, fldname: &String) -> Option<Constant> {
        self.vals.get(fldname).cloned()
    }
}

impl std::hash::Hash for GroupValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let mut hashval = 0;
        for c in self.vals.values() {
            hashval += c.hash_code();
        }
        hashval.hash(state);
    }
}

impl std::cmp::PartialEq for GroupValue {
    fn eq(&self, other: &Self) -> bool {
        for (fldname, v1) in self.vals.iter() {
            let v2 = other.get_val(fldname);
            if Some(v1) != v2.as_ref() {
                return false;
            }
        }
        return true;
    }
}

impl std::cmp::Eq for GroupValue {}
