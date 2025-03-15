use super::{predicate::Predicate, scan::Scan};

pub struct SelectScan<S: Scan> {
    us: S,
    pred: Predicate,
}

impl<S: Scan> SelectScan<S> {
    pub fn new(us: S, pred: Predicate) -> Self {
        SelectScan { us: us, pred }
    }
}

impl<S: Scan> Scan for SelectScan<S> {
    fn before_first(&mut self) -> Result<(), String> {
        self.us.before_first()
    }

    fn next(&mut self) -> Result<bool, String> {
        while self.us.next()? {
            if self.pred.is_satisfied(&self.us) {
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
