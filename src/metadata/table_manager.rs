use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    query::{scan::Scan, update_scan::UpdateScan},
    record::{layout::Layout, schema::Schema, table_scan::TableScan},
    tx::transaction::Transaction,
};

pub const MAX_NAME: i32 = 16;

#[derive(Debug, Clone)]
pub struct TableManager {
    tcat_layout: Arc<Mutex<Layout>>,
    fcat_layout: Arc<Mutex<Layout>>,
}

impl TableManager {
    pub fn new(is_new: bool, tx: Arc<Mutex<Transaction>>) -> Result<Self, String> {
        let mut tcat_schema = Schema::new();
        tcat_schema.add_string_field(&"tblname".to_string(), MAX_NAME)?;
        tcat_schema.add_int_field(&"slotsize".to_string())?;
        let tcat_schema = Arc::new(Mutex::new(tcat_schema));

        let mut fcat_schema = Schema::new();
        fcat_schema.add_string_field(&"tblname".to_string(), MAX_NAME)?;
        fcat_schema.add_string_field(&"fldname".to_string(), MAX_NAME)?;
        fcat_schema.add_int_field(&"type".to_string())?;
        fcat_schema.add_int_field(&"length".to_string())?;
        fcat_schema.add_int_field(&"offset".to_string())?;
        let fcat_schema = Arc::new(Mutex::new(fcat_schema));

        let mut ret = TableManager {
            tcat_layout: Arc::new(Mutex::new(Layout::new_from_schema(tcat_schema.clone())?)),
            fcat_layout: Arc::new(Mutex::new(Layout::new_from_schema(fcat_schema.clone())?)),
        };

        if is_new {
            ret.create_table("tblcat".to_string(), tcat_schema, tx.clone())?;
            ret.create_table("fldcat".to_string(), fcat_schema, tx.clone())?;
        }
        Ok(ret)
    }

    pub fn create_table(
        &mut self,
        tblname: String,
        sch: Arc<Mutex<Schema>>,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<(), String> {
        let layout = Layout::new_from_schema(sch.clone())?;

        let mut tcat = TableScan::new(tx.clone(), "tblcat".to_string(), self.tcat_layout.clone())?;
        tcat.insert()?;
        tcat.set_string("tblname".to_string(), tblname.clone())?;
        tcat.set_int("slotsize".to_string(), layout.slot_size())?;
        tcat.close()?;

        let mut fcat = TableScan::new(tx, "fldcat".to_string(), self.fcat_layout.clone())?;
        let sch = sch.lock().map_err(|_| "failed to get lock")?;
        let binding = sch.fields();
        let fldnames = binding.lock().map_err(|_| "failed to get lock")?;
        for fldname in fldnames.iter() {
            fcat.insert()?;
            fcat.set_string("tblname".to_string(), tblname.clone())?;
            fcat.set_string("fldname".to_string(), fldname.clone())?;
            fcat.set_int("type".to_string(), sch.field_type(fldname)?)?;
            fcat.set_int("length".to_string(), sch.length(fldname)?)?;
            fcat.set_int("offset".to_string(), layout.offset(fldname)? as i32)?;
        }
        fcat.close()?;

        Ok(())
    }

    pub fn get_layout(
        &self,
        tblname: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Layout, String> {
        let mut size = -1;
        let mut tcat = TableScan::new(tx.clone(), "tblcat".to_string(), self.tcat_layout.clone())?;
        while tcat.next()? {
            if tcat.get_string(&"tblname".to_string())?.eq(&tblname) {
                size = tcat.get_int(&"slotsize".to_string())?;
                break;
            }
        }
        tcat.close()?;

        let mut sch = Schema::new();
        let mut offsets = HashMap::<String, usize>::new();
        let mut fcat = TableScan::new(tx.clone(), "fldcat".to_string(), self.fcat_layout.clone())?;
        while fcat.next()? {
            if fcat.get_string(&"tblname".to_string())?.eq(&tblname) {
                let fldname = fcat.get_string(&"fldname".to_string())?;
                let fldtype = fcat.get_int(&"type".to_string())?;
                let fldlen = fcat.get_int(&"length".to_string())?;
                let offset = fcat.get_int(&"offset".to_string())?;
                offsets.insert(fldname.clone(), offset as usize);
                sch.add_field(&fldname, fldtype, fldlen)?;
            }
        }
        fcat.close()?;
        let ret = Layout::new(
            Arc::new(Mutex::new(sch)),
            Arc::new(Mutex::new(offsets)),
            size,
        );

        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::TempDir;

    use crate::{
        record::schema::{
            field_type::{INTEGER, VARCHAR},
            Schema,
        },
        server::simple_db::SimpleDB,
    };

    use super::TableManager;

    #[test]
    fn test_table_manager() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(SimpleDB::new(temp_dir.path(), 400, 8));
        let tx = db.new_tx();
        let mut tm = TableManager::new(true, tx.clone()).unwrap();

        let mut sch = Schema::new();
        sch.add_int_field(&"A".to_string()).unwrap();
        sch.add_string_field(&"B".to_string(), 9).unwrap();
        let sch = Arc::new(Mutex::new(sch));
        tm.create_table("MyTable".to_string(), sch.clone(), tx.clone())
            .unwrap();

        let layout = tm.get_layout("MyTable".to_string(), tx.clone()).unwrap();
        let size = layout.slot_size();
        let binding = layout.schema();
        let sch2 = binding.lock().unwrap();
        println!("MyTable has slot size {}", size);
        assert!(size == 21);
        println!("Its fields are:");
        let binding = sch2.fields();
        let binding = binding.lock().unwrap();
        let fldnames = binding.iter();
        for fldname in fldnames {
            if fldname.eq("A") {
                println!("A : int");
                assert_eq!(sch2.field_type(fldname), Ok(INTEGER));
            } else if fldname.eq("B") {
                let strlen = sch2.length(fldname).unwrap();
                println!("B : varchar({})", strlen);
                assert_eq!(sch2.field_type(fldname), Ok(VARCHAR));
                assert_eq!(strlen, 9);
            } else {
                panic!("unreachable!!");
            }
        }
        tx.lock().unwrap().commit().unwrap();
    }
}
