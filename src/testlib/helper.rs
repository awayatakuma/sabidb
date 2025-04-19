use crate::server::simple_db::SimpleDB;

pub fn create_student_data(db: &mut SimpleDB) {
    let tx = db.new_tx();

    let planner = db.planner.as_mut().unwrap();

    // add students
    let mut cmd = "create table students(sid int, sname varchar(9),majorid int, gradyear int)";
    planner.execute_update(cmd, tx.clone()).unwrap();

    cmd = "insert into students(sid, sname, majorid, gradyear) values";
    let students = [
        "(1, 'joe', 10, 2021)",
        "(2, 'amy', 20, 2020)",
        "(3, 'max', 10, 2022)",
        "(4, 'sue', 20, 2022)",
        "(5, 'bob', 30, 2020)",
        "(6, 'kim', 20, 2020)",
        "(7, 'art', 30, 2021)",
        "(8, 'pat', 20, 2019)",
        "(9, 'lee', 10, 2021)",
    ];

    for student in students {
        let s = format!("{} {}", cmd, student);
        planner.execute_update(&s, tx.clone()).unwrap();
    }

    // departments
    let mut cmd = "create table depts(did int, dname varchar(8))";
    planner.execute_update(cmd, tx.clone()).unwrap();

    cmd = "insert into depts(did, dname) values";
    let depts = ["(10, 'compsci')", "(20, 'math')", "(30, 'drama')"];

    for dept in depts {
        let s = format!("{} {}", cmd, dept);
        planner.execute_update(&s, tx.clone()).unwrap();
    }

    // courses
    let mut cmd = "create table courses(cid int, title varchar(10), deptid int)";
    planner.execute_update(cmd, tx.clone()).unwrap();

    cmd = "insert into courses(cid, title, deptid) values";
    let courses = [
        "(12, 'db systems', 10)",
        "(22, 'compilers', 10)",
        "(32, 'calculus', 20)",
        "(42, 'algebra', 20)",
        "(52, 'acting', 30)",
        "(62, 'elocution', 30)",
    ];

    for course in courses {
        let s = format!("{} {}", cmd, course);
        planner.execute_update(&s, tx.clone()).unwrap();
    }

    // sections
    let mut cmd =
        "create table sections(sectid int, courseid int, prof varchar(8), yearoffered int)";
    planner.execute_update(cmd, tx.clone()).unwrap();

    cmd = "insert into sections(sectid, courseid, prof, yearoffered) values";
    let sections = [
        "(13, 12, 'turing', 2018)",
        "(23, 12, 'turing', 2019)",
        "(33, 32, 'newton', 2019)",
        "(43, 32, 'einstein', 2017)",
        "(53, 62, 'brando', 2018)",
    ];

    for section in sections {
        let s = format!("{} {}", cmd, section);
        planner.execute_update(&s, tx.clone()).unwrap();
    }

    // enroll
    let mut cmd = "create table enrolls(eid int, studentid int, sectionid int, grade varchar(2))";
    planner.execute_update(cmd, tx.clone()).unwrap();

    cmd = "insert into enrolls(eid, studentid, sectionid, grade) values";
    let enrolls = [
        "(14, 1, 13, 'A')",
        "(24, 1, 43, 'C' )",
        "(34, 2, 43, 'B+')",
        "(44, 4, 33, 'B' )",
        "(54, 4, 53, 'A' )",
        "(64, 6, 53, 'A' )",
    ];

    for enroll in enrolls {
        let s = format!("{} {}", cmd, enroll);
        planner.execute_update(&s, tx.clone()).unwrap();
    }

    // index
    // students -> majorid
    let cmd = "create index majorid_idx on students(majorid)";
    planner.execute_update(cmd, tx.clone()).unwrap();

    // enrolls -> studentid
    let cmd = "create index studentid_idx on enrolls(studentid)";
    planner.execute_update(cmd, tx.clone()).unwrap();

    tx.lock().unwrap().commit().unwrap();
}
