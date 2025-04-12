use crate::query::{predicate::Predicate, scan::Scan, select_scan::SelectScan};

use super::plan::Plan;

pub struct SelectPlan {
    p: Box<dyn Plan>,
    pred: Predicate,
}

impl Plan for SelectPlan {
    fn open(&mut self) -> Result<Box<dyn Scan>, String> {
        let s = self.p.as_mut().open()?;
        Ok(Box::new(SelectScan::new(s, self.pred.clone())))
    }
    fn blocks_accessed(&self) -> Result<i32, String> {
        Ok(self.p.blocks_accessed()?)
    }

    fn records_output(&self) -> Result<i32, String> {
        Ok(self.p.records_output()? / self.pred.reduction_factor(&self.p))
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        if self.pred.equate_with_constant(fldname.clone()).is_some() {
            Ok(1)
        } else {
            if let Some(fldname2) = self.pred.equate_with_field(fldname.clone()) {
                Ok(i32::min(
                    self.p.distinct_values(fldname)?,
                    self.p.distinct_values(fldname2)?,
                ))
            } else {
                Ok(self.p.distinct_values(fldname)?)
            }
        }
    }

    fn schema(&self) -> crate::record::schema::Schema {
        self.p.schema()
    }
}

impl SelectPlan {
    pub fn new(p: Box<dyn Plan>, pred: Predicate) -> Self {
        SelectPlan { p: p, pred: pred }
    }
}
