use super::{predicate::Predicate, scan::Scan, update_scan::UpdateScan};

pub struct SelectScan {
    s: Box<dyn Scan>,
    pred: Predicate,
}

impl SelectScan {
    pub fn new(s: Box<dyn Scan>, pred: Predicate) -> Self {
        SelectScan { s: s, pred }
    }
}

impl Scan for SelectScan {
    fn before_first(&mut self) -> Result<(), String> {
        self.s.before_first()
    }

    fn next(&mut self) -> Result<bool, String> {
        while self.s.next()? {
            if self.pred.is_satisfied(&self.s) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        self.s.get_int(fldname)
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        self.s.get_string(fldname)
    }

    fn get_val(&self, fldname: &String) -> Result<super::constant::Constant, String> {
        self.s.get_val(fldname)
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        self.s.has_field(fldname)
    }

    fn close(&mut self) -> Result<(), String> {
        self.s.close()
    }

    fn to_update_scan(&mut self) -> Result<&mut dyn super::update_scan::UpdateScan, String> {
        Ok(self)
    }
}

impl UpdateScan for SelectScan {
    fn set_val(&mut self, fldname: String, val: super::constant::Constant) -> Result<(), String> {
        self.s.to_update_scan()?.set_val(fldname, val)
    }

    fn set_int(&mut self, fldname: String, val: i32) -> Result<(), String> {
        self.s.to_update_scan()?.set_int(fldname, val)
    }

    fn set_string(&mut self, fldname: String, val: String) -> Result<(), String> {
        self.s.to_update_scan()?.set_string(fldname, val)
    }

    fn insert(&mut self) -> Result<(), String> {
        self.s.to_update_scan()?.insert()
    }

    fn delete(&mut self) -> Result<(), String> {
        self.s.to_update_scan()?.delete()
    }

    fn get_rid(&mut self) -> Result<crate::record::rid::RID, String> {
        self.s.to_update_scan()?.get_rid()
    }

    fn move_to_rid(&mut self, rid: crate::record::rid::RID) -> Result<(), String> {
        self.s.to_update_scan()?.move_to_rid(rid)
    }

    fn to_scan(&mut self) -> Result<&mut dyn Scan, String> {
        Ok(self)
    }
}
