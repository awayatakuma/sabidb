use super::{
    predicate::Predicate,
    scan::{RefScanType, Scan},
};

pub struct SelectScan {
    us: Box<dyn Scan>,
    pred: Predicate,
}

impl SelectScan {
    pub fn new(us: Box<dyn Scan>, pred: Predicate) -> Self {
        SelectScan { us: us, pred }
    }
}

impl Scan for SelectScan {
    fn before_first(&mut self) -> Result<(), String> {
        self.us.before_first()
    }

    fn next(&mut self) -> Result<bool, String> {
        while self.us.next()? {
            let ref_s = RefScanType::Scan(&self.us);
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
