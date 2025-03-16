#[derive(Debug, Clone)]
pub struct CreateIndexData {
    idxname: String,
    tblname: String,
    fldname: String,
}

impl CreateIndexData {
    pub fn new(idxname: String, tblname: String, fldname: String) -> Self {
        CreateIndexData {
            idxname: idxname,
            tblname: tblname,
            fldname: fldname,
        }
    }

    pub fn idx_name(&self) -> String {
        self.idxname.clone()
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn field_name(&self) -> String {
        self.fldname.clone()
    }
}
