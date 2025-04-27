use crate::query::{expression::Expression, predicate::Predicate};

#[derive(Debug, Clone)]
pub struct ModifyData {
    tblname: String,
    fldname: String,
    newval: Expression,
    pred: Predicate,
}

impl ModifyData {
    pub fn new(tblname: String, fldname: String, newval: Expression, pred: Predicate) -> Self {
        ModifyData {
            tblname: tblname,
            fldname: fldname,
            newval: newval,
            pred: pred,
        }
    }
    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn target_field(&self) -> String {
        self.fldname.clone()
    }

    pub fn new_val(&self) -> Expression {
        self.newval.clone()
    }

    pub fn pred(&self) -> Predicate {
        self.pred.clone()
    }
}
