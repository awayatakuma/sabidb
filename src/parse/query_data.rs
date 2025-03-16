use core::fmt;

use crate::query::predicate::Predicate;

#[derive(Debug, Clone)]
pub struct QueryData {
    fields: Vec<String>,
    tables: Vec<String>,
    pred: Predicate,
}

impl fmt::Display for QueryData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let fields = self.fields.join(", ");
        let tables = self.tables.join(", ");
        let res = format!("select {} from {}", fields, tables);
        let predstring = self.pred.to_string();
        if predstring.is_empty() {
            write!(f, "{}", res)
        } else {
            write!(f, "{}", format!("{} where {}", res, predstring))
        }
    }
}

impl QueryData {
    pub fn new(fields: Vec<String>, tables: Vec<String>, pred: Predicate) -> Self {
        QueryData {
            fields: fields,
            tables: tables,
            pred: pred,
        }
    }
    pub fn tables(&self) -> Vec<String> {
        self.tables.clone()
    }

    pub fn fields(&self) -> Vec<String> {
        self.fields.clone()
    }

    pub fn pred(&self) -> Predicate {
        self.pred.clone()
    }
}
