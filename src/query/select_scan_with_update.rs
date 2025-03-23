use super::{
    predicate::Predicate,
    scan::{RefScanType, Scan},
    update_scan::UpdateScan,
};

pub struct SelectScanWithUpdate {
    us: Box<dyn UpdateScan>,
    pred: Predicate,
}

impl SelectScanWithUpdate {
    pub fn new(us: Box<dyn UpdateScan>, pred: Predicate) -> Self {
        SelectScanWithUpdate { us: us, pred }
    }
}

impl Scan for SelectScanWithUpdate {
    fn before_first(&mut self) -> Result<(), String> {
        self.us.before_first()
    }

    fn next(&mut self) -> Result<bool, String> {
        while self.us.next()? {
            let ref_s = RefScanType::UpdateScan(&self.us);
            if self.pred.is_satisfied(&ref_s) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        self.us.get_int(fldname)
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        self.us.get_string(fldname)
    }

    fn get_val(&self, fldname: &String) -> Result<super::constant::Constant, String> {
        self.us.get_val(fldname)
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        self.us.has_field(fldname)
    }

    fn close(&mut self) -> Result<(), String> {
        self.us.close()
    }
}

impl UpdateScan for SelectScanWithUpdate {
    fn set_val(&mut self, fldname: String, val: super::constant::Constant) -> Result<(), String> {
        self.us.set_val(fldname, val)
    }

    fn set_int(&mut self, fldname: String, val: i32) -> Result<(), String> {
        self.us.set_int(fldname, val)
    }

    fn set_string(&mut self, fldname: String, val: String) -> Result<(), String> {
        self.us.set_string(fldname, val)
    }

    fn insert(&mut self) -> Result<(), String> {
        self.us.insert()
    }

    fn delete(&mut self) -> Result<(), String> {
        self.us.delete()
    }

    fn get_rid(&mut self) -> Result<crate::record::rid::RID, String> {
        self.us.get_rid()
    }

    fn move_to_rid(&mut self, rid: crate::record::rid::RID) -> Result<(), String> {
        self.us.move_to_rid(rid)
    }
}
