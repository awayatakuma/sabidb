#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use crate::server::simple_db::SimpleDB;
    use crate::query::scan::Scan;

    fn run_update(planner: &mut crate::plan::planner::Planner, sql: &str, tx: std::sync::Arc<std::sync::Mutex<crate::tx::transaction::Transaction>>) {
        println!("SQL: {}", sql);
        planner.execute_update(sql, tx).expect(&format!("Failed to execute: {}", sql));
    }

    #[test]
    fn test_comprehensive_sql_operations() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new(temp_dir.path());
        let tx = db.new_tx();
        let mut planner = db.planner.unwrap();

        println!("\n--- Starting Comprehensive SQL Integration Test ---");

        // 1. Create tables
        run_update(&mut planner, "create table students(sid int, sname varchar(9), majorid int, gradyear int)", tx.clone());
        run_update(&mut planner, "create table depts(did int, dname varchar(8))", tx.clone());

        // 2. Create index
        run_update(&mut planner, "create index majorid_idx on students(majorid)", tx.clone());

        // 3. Insert data into students
        run_update(&mut planner, "insert into students(sid, sname, majorid, gradyear) values (1, 'joe', 10, 2021)", tx.clone());
        run_update(&mut planner, "insert into students(sid, sname, majorid, gradyear) values (2, 'amy', 20, 2020)", tx.clone());
        run_update(&mut planner, "insert into students(sid, sname, majorid, gradyear) values (3, 'max', 10, 2022)", tx.clone());
        run_update(&mut planner, "insert into students(sid, sname, majorid, gradyear) values (4, 'sue', 20, 2022)", tx.clone());
        run_update(&mut planner, "insert into students(sid, sname, majorid, gradyear) values (5, 'bob', 30, 2020)", tx.clone());

        // 4. Insert data into depts
        run_update(&mut planner, "insert into depts(did, dname) values (10, 'compsci')", tx.clone());
        run_update(&mut planner, "insert into depts(did, dname) values (20, 'math')", tx.clone());
        run_update(&mut planner, "insert into depts(did, dname) values (30, 'drama')", tx.clone());

        // 5. Select all students and verify values
        let qry = "select sid, sname, majorid, gradyear from students".to_string();
        println!("SQL: {}", qry);
        let plan = planner.create_query_planner(&qry, tx.clone()).unwrap();
        let scan = plan.lock().unwrap().open().unwrap();
        let mut count = 0;
        let mut results = Vec::new();
        while scan.lock().unwrap().next().unwrap() {
            let sid = scan.lock().unwrap().get_int(&"sid".to_string()).unwrap();
            let sname = scan.lock().unwrap().get_string(&"sname".to_string()).unwrap();
            results.push((sid, sname));
            count += 1;
        }
        assert_eq!(count, 5);
        results.sort_by_key(|r| r.0);
        assert_eq!(results[0], (1, "joe".to_string()));
        assert_eq!(results[4], (5, "bob".to_string()));

        // 6. Select with where clause and verify values
        let qry = "select sid, sname from students where majorid = 10".to_string();
        println!("SQL: {}", qry);
        let plan = planner.create_query_planner(&qry, tx.clone()).unwrap();
        let scan = plan.lock().unwrap().open().unwrap();
        let mut count = 0;
        while scan.lock().unwrap().next().unwrap() {
            let sid = scan.lock().unwrap().get_int(&"sid".to_string()).unwrap();
            let sname = scan.lock().unwrap().get_string(&"sname".to_string()).unwrap();
            if sid == 1 { assert_eq!(sname, "joe"); }
            else if sid == 3 { assert_eq!(sname, "max"); }
            else { panic!("Unexpected sid: {}", sid); }
            count += 1;
        }
        assert_eq!(count, 2);

        // 7. Join select and verify values
        let qry = "select sname, dname from students, depts where majorid = did".to_string();
        println!("SQL: {}", qry);
        let plan = planner.create_query_planner(&qry, tx.clone()).unwrap();
        let scan = plan.lock().unwrap().open().unwrap();
        let mut count = 0;
        while scan.lock().unwrap().next().unwrap() {
            let sname = scan.lock().unwrap().get_string(&"sname".to_string()).unwrap();
            let dname = scan.lock().unwrap().get_string(&"dname".to_string()).unwrap();
            match sname.as_str() {
                "joe" | "max" => assert_eq!(dname, "compsci"),
                "amy" | "sue" => assert_eq!(dname, "math"),
                "bob" => assert_eq!(dname, "drama"),
                _ => panic!("Unexpected student: {}", sname),
            }
            count += 1;
        }
        assert_eq!(count, 5);

        // 8. Create view
        run_update(&mut planner, "create view cs_students as select sid, sname from students where majorid = 10", tx.clone());

        // 9. Select from view and verify values
        let qry = "select sid, sname from cs_students".to_string();
        println!("SQL: {}", qry);
        let plan = planner.create_query_planner(&qry, tx.clone()).unwrap();
        let scan = plan.lock().unwrap().open().unwrap();
        let mut count = 0;
        while scan.lock().unwrap().next().unwrap() {
            let sname = scan.lock().unwrap().get_string(&"sname".to_string()).unwrap();
            assert!(sname == "joe" || sname == "max");
            count += 1;
        }
        assert_eq!(count, 2);

        // 10. Update and verify value-level change
        run_update(&mut planner, "update students set gradyear = 2023 where sid = 1", tx.clone());

        let qry = "select sid, sname, gradyear from students where sid = 1".to_string();
        println!("SQL: {}", qry);
        let plan = planner.create_query_planner(&qry, tx.clone()).unwrap();
        let scan = plan.lock().unwrap().open().unwrap();
        assert!(scan.lock().unwrap().next().unwrap(), "Should find updated student");
        let gy = scan.lock().unwrap().get_int(&"gradyear".to_string()).unwrap();
        let sn = scan.lock().unwrap().get_string(&"sname".to_string()).unwrap();
        assert_eq!(gy, 2023, "Gradyear should be updated to 2023");
        assert_eq!(sn, "joe");
        scan.lock().unwrap().close().unwrap();

        // 11. Delete and verify removal
        run_update(&mut planner, "delete from students where sid = 5", tx.clone());

        let qry = "select sid, sname from students".to_string();
        println!("SQL: {}", qry);
        let plan = planner.create_query_planner(&qry, tx.clone()).unwrap();
        let scan = plan.lock().unwrap().open().unwrap();
        let mut count = 0;
        while scan.lock().unwrap().next().unwrap() {
            let sid = scan.lock().unwrap().get_int(&"sid".to_string()).unwrap();
            assert!(sid != 5, "Sid 5 should have been deleted");
            count += 1;
        }
        assert_eq!(count, 4);

        tx.lock().unwrap().commit().unwrap();
        println!("--- Comprehensive SQL Integration Test Passed ---\n");
    }
}
