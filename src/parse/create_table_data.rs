use crate::record::schema::Schema;

#[derive(Debug, Clone)]
pub struct CreateTableData {
    tblname: String,
    sch: Schema,
}

impl CreateTableData {
    pub fn new(tblname: String, sch: Schema) -> Self {
        CreateTableData {
            tblname: tblname,
            sch: sch,
        }
    }

    pub fn table_name(&self) -> String {
        self.tblname.clone()
    }

    pub fn new_schema(&self) -> Schema {
        self.sch.clone()
    }
}
