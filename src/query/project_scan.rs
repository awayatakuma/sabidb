use super::scan::Scan;

pub struct ProjectScan {
    s: Box<dyn Scan>,
    fieldlist: Vec<String>,
}

impl ProjectScan {
    pub fn new(s: Box<dyn Scan>, fieldlist: Vec<String>) -> Self {
        ProjectScan {
            s: s,
            fieldlist: fieldlist,
        }
    }
}

impl Scan for ProjectScan {
    fn before_first(&mut self) -> Result<(), String> {
        self.s.before_first()
    }

    fn next(&mut self) -> Result<bool, String> {
        self.s.next()
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        if self.has_field(fldname)? {
            return self.s.get_int(fldname);
        }
        Err(format!("field {} not found", fldname))
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        if self.has_field(fldname)? {
            return self.s.get_string(fldname);
        }
        Err(format!("field {} not found", fldname))
    }

    fn get_val(&self, fldname: &String) -> Result<super::constant::Constant, String> {
        if self.has_field(fldname)? {
            return self.s.get_val(fldname);
        }
        Err(format!("field {} not found", fldname))
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        Ok(self.fieldlist.contains(fldname))
    }

    fn close(&mut self) -> Result<(), String> {
        self.s.close()
    }

    fn to_update_scan(&mut self) -> Result<&mut dyn super::update_scan::UpdateScan, String> {
        Err("Unexpected downcast".to_string())
    }
}
