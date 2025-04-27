use crate::{query::constant::Constant, record::rid::RID};

pub(crate) trait Index {
    fn before_first(&mut self, search_key: &Constant) -> Result<(), String>;
    fn next(&mut self) -> Result<bool, String>;
    fn get_data_rid(&self) -> Result<RID, String>;
    fn insert(&mut self, dataval: &Constant, datarid: RID) -> Result<(), String>;
    fn delete(&mut self, dataval: &Constant, datarid: RID) -> Result<(), String>;
    fn close(&mut self) -> Result<(), String>;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use crate::{
        plan::{plan::Plan, table_plan::TablePlan},
        query::constant::Constant,
        server::simple_db::SimpleDB,
        testlib::helper::create_student_data,
    };

    #[test]
    fn test_index_retrival() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SimpleDB::new(temp_dir.path());
        create_student_data(&mut db);

        let tx = db.new_tx();
        let mdm = db.metadata_manager();

        let studentplan = TablePlan::new(tx.clone(), "students".to_string(), mdm.clone()).unwrap();
        let binding = studentplan
            .open()
            .unwrap()
            .lock()
            .unwrap()
            .to_update_scan()
            .unwrap();
        let mut studentscan = binding.lock().unwrap();

        let indexes = mdm
            .lock()
            .unwrap()
            .get_index_info("students".to_string(), tx.clone())
            .unwrap();
        let ii = indexes.get("majorid").unwrap();
        let binding = ii.open().unwrap();
        let mut idx = binding.lock().unwrap();

        idx.before_first(&Constant::new_from_i32(20)).unwrap();

        // Unreachable even if you implement ch12 because index insertion is not yet implemented
        while idx.next().unwrap() {
            let datarid = idx.get_data_rid().unwrap();
            studentscan.move_to_rid(datarid).unwrap();
            println!(
                "{}",
                studentscan.get_string(&"majorid".to_string()).unwrap()
            );
        }

        idx.close().unwrap();
        studentscan.close().unwrap();
        tx.lock().unwrap().commit().unwrap();
    }

    #[test]
    fn test_index_update() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SimpleDB::new(temp_dir.path());
        create_student_data(&mut db);

        let tx = db.new_tx();
        let mdm = db.metadata_manager();

        let studentplan = TablePlan::new(tx.clone(), "students".to_string(), mdm.clone()).unwrap();
        let binding = studentplan
            .open()
            .unwrap()
            .lock()
            .unwrap()
            .to_update_scan()
            .unwrap();
        let mut studentscan = binding.lock().unwrap();

        // Create a map containing all indexes for STUDENT.
        let mut indexes = HashMap::new();
        let idxinfo = mdm
            .lock()
            .unwrap()
            .get_index_info("students".to_string(), tx.clone())
            .unwrap();
        for fldname in idxinfo.keys() {
            let idx = idxinfo.get(fldname).unwrap().open().unwrap();
            indexes.insert(fldname.clone(), idx);
        }

        // Task 1: insert a new STUDENT record for Sam
        //    First, insert the record into STUDENT.
        studentscan.insert().unwrap();
        studentscan.set_int("sid".to_string(), 11).unwrap();
        studentscan
            .set_string("sname".to_string(), "sam".to_string())
            .unwrap();
        studentscan.set_int("gradyear".to_string(), 2023).unwrap();
        studentscan.set_int("majorid".to_string(), 30).unwrap();

        //    Then insert a record into each of the indexes.
        let datarid = studentscan.get_rid().unwrap();
        for fldname in indexes.keys() {
            let dataval = studentscan.get_val(fldname).unwrap();
            let idx = indexes.get(fldname).unwrap();
            idx.lock()
                .unwrap()
                .insert(&dataval, datarid.clone())
                .unwrap();
        }

        // Task 2: find and delete Joe's record
        studentscan.before_first().unwrap();
        while studentscan.next().unwrap() {
            if studentscan
                .get_string(&"sname".to_string())
                .unwrap()
                .eq(&"joe")
            {
                // First, delete the index records for Joe.
                let joe_rid = studentscan.get_rid().unwrap();
                for fldname in indexes.keys() {
                    let dataval = studentscan.get_val(fldname).unwrap();
                    let idx = indexes.get(fldname).unwrap();
                    idx.lock()
                        .unwrap()
                        .delete(&dataval, joe_rid.clone())
                        .unwrap();
                }

                studentscan.delete().unwrap();
                break;
            }
        }

        // Print the records to verify the updates
        studentscan.before_first().unwrap();
        while studentscan.next().unwrap() {
            let sname = studentscan.get_string(&"sname".to_string()).unwrap();
            let sid = studentscan.get_int(&"sid".to_string()).unwrap();
            println!("{} {}", sname, sid);
            assert_ne!(sname, "joe");
        }

        studentscan.close().unwrap();

        for idx in indexes.values() {
            idx.lock().unwrap().close().unwrap();
        }

        tx.lock().unwrap().commit().unwrap();
    }
}
