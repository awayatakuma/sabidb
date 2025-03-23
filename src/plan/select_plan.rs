use crate::query::{
    predicate::Predicate, select_scan::SelectScan, select_scan_with_update::SelectScanWithUpdate,
};

use super::plan::Plan;

pub struct SelectPlan {
    p: Box<dyn Plan>,
    pred: Predicate,
}

impl Plan for SelectPlan {
    fn open(&mut self, is_mutable: bool) -> crate::query::scan::ScanType {
        match self.p.open(is_mutable) {
            crate::query::scan::ScanType::Scan(scan) => crate::query::scan::ScanType::Scan(
                Box::new(SelectScan::new(scan, self.pred.clone())),
            ),
            crate::query::scan::ScanType::UpdateScan(update_scan) => {
                crate::query::scan::ScanType::UpdateScan(Box::new(SelectScanWithUpdate::new(
                    update_scan,
                    self.pred.clone(),
                )))
            }
        }
    }
    fn blocks_accessed(&self) -> i32 {
        self.p.blocks_accessed()
    }

    fn records_output(&self) -> i32 {
        self.p.records_output() / self.pred.reduction_factor(&self.p)
    }

    fn distinct_values(&self, fldname: String) -> i32 {
        if self.pred.equate_with_constant(fldname.clone()).is_some() {
            return 1;
        } else {
            if let Some(fldname2) = self.pred.equate_with_field(fldname.clone()) {
                return i32::min(
                    self.p.distinct_values(fldname),
                    self.p.distinct_values(fldname2),
                );
            } else {
                return self.p.distinct_values(fldname);
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
