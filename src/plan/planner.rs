use std::sync::{Arc, Mutex};

use crate::{
    parse::{lexer::BadSyntaxException, parser::Parser, query_data::QueryData},
    tx::transaction::Transaction,
};

use super::{plan::Plan, query_planner::QueryPlanner, update_planner::UpdatePlanner};

pub struct Planner {
    qplanner: Arc<Mutex<dyn QueryPlanner>>,
    uplanner: Arc<Mutex<dyn UpdatePlanner>>,
}

impl Planner {
    pub fn new(
        qplanner: Arc<Mutex<dyn QueryPlanner>>,
        uplanner: Arc<Mutex<dyn UpdatePlanner>>,
    ) -> Self {
        Planner {
            qplanner: qplanner,
            uplanner: uplanner,
        }
    }

    pub fn create_query_planner(
        &mut self,
        qry: String,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<Arc<Mutex<dyn Plan>>, super::super::parse::lexer::BadSyntaxException> {
        let mut parser = Parser::new(&qry);
        let data = parser.query()?;
        // Self::verify_query(&data);

        self.qplanner
            .lock()
            .map_err(|_| BadSyntaxException {})?
            .create_plan(data, tx)
            .map_err(|_| BadSyntaxException {})
    }

    pub fn execute_update(
        &mut self,
        cmd: &str,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32, super::super::parse::lexer::BadSyntaxException> {
        let mut p = Parser::new(cmd);
        // Self::verify_update(&data);
        match p.update_cmd()? {
            crate::parse::parser::UpdateCommand::Insert(insert_data) => self
                .uplanner
                .lock()
                .map_err(|_| BadSyntaxException {})?
                .execute_insert(insert_data, tx)
                .map_err(|_| BadSyntaxException),
            crate::parse::parser::UpdateCommand::Delete(delete_data) => self
                .uplanner
                .lock()
                .map_err(|_| BadSyntaxException {})?
                .execute_delete(delete_data, tx)
                .map_err(|_| BadSyntaxException),
            crate::parse::parser::UpdateCommand::Modify(modify_data) => self
                .uplanner
                .lock()
                .map_err(|_| BadSyntaxException {})?
                .execute_modify(modify_data, tx)
                .map_err(|_| BadSyntaxException),
            crate::parse::parser::UpdateCommand::CreateTable(create_table_data) => self
                .uplanner
                .lock()
                .map_err(|_| BadSyntaxException {})?
                .execute_create_table(create_table_data, tx)
                .map_err(|_| BadSyntaxException),
            crate::parse::parser::UpdateCommand::CreateView(create_view_data) => self
                .uplanner
                .lock()
                .map_err(|_| BadSyntaxException {})?
                .execute_create_view(create_view_data, tx)
                .map_err(|_| BadSyntaxException),
            crate::parse::parser::UpdateCommand::CreateIndex(create_index_data) => self
                .uplanner
                .lock()
                .map_err(|_| BadSyntaxException {})?
                .execute_create_index(create_index_data, tx)
                .map_err(|_| BadSyntaxException),
        }
    }

    // SimpleDB does not verify queries, although it should.
    fn _verify_query(_data: &QueryData) {}

    // SimpleDB does not verify queries, although it should.
    // fn verify_update(_data: Object) {}
}

#[cfg(test)]
mod tests {

    use std::{
        cell::RefCell,
        rc::Rc,
        sync::{Arc, Mutex},
    };

    use rand::Rng;
    use tempfile::TempDir;

    use crate::{
        plan::{
            plan::Plan, product_plan::ProductPlan, project_plan::ProjectPlan,
            select_plan::SelectPlan, table_plan::TablePlan,
        },
        query::{constant::Constant, expression::Expression, predicate::Predicate, term::Term},
        server::simple_db::SimpleDB,
    };

    #[test]
    fn test_planner1() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new(temp_dir.path());
        let tx = db.new_tx();
        let mut planner = db.planner.unwrap();

        let cmd = "create table TT(A int, B varchar(9))";
        planner.execute_update(cmd, tx.clone()).unwrap();

        let n = 200;
        let mut expected = 0;
        let mut rng = rand::rng();
        for _ in 0..n {
            let a = rng.random_range(0..=50);
            let b = format!("rec{}", a);
            let cmd = format!("insert into TT(A,B) values({}, '{}')", a, b);
            planner.execute_update(&cmd, tx.clone()).unwrap();
            if a == 10 {
                expected += 1;
            }
        }

        let mut actual = 0;

        let qry = "select B from TT where A=10";
        let p = planner
            .create_query_planner(qry.to_string(), tx.clone())
            .unwrap();
        let s = p.lock().unwrap().open().unwrap();

        while s.lock().unwrap().next().unwrap() {
            println!(
                "{}",
                s.lock().unwrap().get_string(&"B".to_string()).unwrap()
            );
            actual += 1;
        }
        s.lock().unwrap().close().unwrap();
        tx.lock().unwrap().commit().unwrap();

        assert_eq!(expected, actual)
    }

    #[test]
    fn test_planner2() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new(temp_dir.path());
        let tx = db.new_tx();
        let mut planner = db.planner.unwrap();

        let cmd = "create table T(A int, B varchar(9))";
        planner.execute_update(cmd, tx.clone()).unwrap();

        let n = 200;
        for i in 0..n {
            let a = i;
            let b = format!("bbb{}", a);
            let cmd = format!("insert into T(A,B) values({}, '{}')", a, b);
            planner.execute_update(&cmd, tx.clone()).unwrap();
        }

        let cmd = "create table TT(C int, D varchar(9))";
        planner.execute_update(cmd, tx.clone()).unwrap();

        let n = 200;
        for i in 0..n {
            let c = i;
            let d = format!("ddd{}", c);
            let cmd = format!("insert into TT(C,D) values({}, '{}')", c, d);
            planner.execute_update(&cmd, tx.clone()).unwrap();
        }

        let qry = "select A,B,C,D from T,TT where A=C";
        let p = planner
            .create_query_planner(qry.to_string(), tx.clone())
            .unwrap();
        let s = p.lock().unwrap().open().unwrap();
        let mut locked_s = s.lock().unwrap();
        while locked_s.next().unwrap() {
            println!(
                "{} {}",
                locked_s.get_string(&"B".to_string()).unwrap(),
                locked_s.get_string(&"D".to_string()).unwrap()
            );
            assert_eq!(
                locked_s.get_int(&"A".to_string()),
                locked_s.get_int(&"C".to_string())
            )
        }
        locked_s.close().unwrap();
        tx.lock().unwrap().commit().unwrap();
    }

    #[test]
    fn test_planner_student() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new(temp_dir.path());
        let tx = db.new_tx();
        let mut planner = db.planner.unwrap();

        let cmd = "create table student(sname varchar(9), gradyear int)";
        planner.execute_update(cmd, tx.clone()).unwrap();

        let n = 200;
        for i in 0..n {
            let a = i;
            let b = format!("bbb{}", a);
            let cmd = format!("insert into student(sname,gradyear) values('{}', {})", b, a);
            planner.execute_update(&cmd, tx.clone()).unwrap();
        }

        let cmd = "delete from student where gradyear = 30";
        let num = planner.execute_update(cmd, tx.clone()).unwrap();

        tx.lock().unwrap().commit().unwrap();
        assert!(num == 1);
    }

    #[test]
    fn test_single_table_plan() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new(temp_dir.path());
        let mdm = Arc::new(Mutex::new(db.metadata_manager()));
        let tx = db.new_tx();

        let mut planner = db.planner.unwrap();

        let cmd = "create table student(sname varchar(9),majorid int, gradyear int)";
        planner.execute_update(cmd, tx.clone()).unwrap();

        let n = 200;
        for i in 0..n {
            let a = i;
            let b = format!("bbb{}", a);
            let cmd = format!("insert into student(sname,gradyear) values('{}', {})", b, a);
            planner.execute_update(&cmd, tx.clone()).unwrap();
        }

        let p1 = Arc::new(Mutex::new(
            TablePlan::new(tx.clone(), "student".to_string(), mdm).unwrap(),
        ));
        print_stats(1, p1.clone());

        let t = Term::new(
            Expression::new_from_fldname("majorid".to_string()),
            Expression::new_from_val(Constant::new_from_i32(10)),
        );
        let pred = Predicate::new_from_term(t);
        let p2 = Arc::new(Mutex::new(SelectPlan::new(p1, pred)));
        print_stats(2, p2.clone());

        let t2 = Term::new(
            Expression::new_from_fldname("gradyear".to_string()),
            Expression::new_from_val(Constant::new_from_i32(2020)),
        );
        let pred2 = Predicate::new_from_term(t2);
        let p3 = Arc::new(Mutex::new(SelectPlan::new(p2, pred2)));
        print_stats(3, p3.clone());

        let c = vec![
            "sname".to_string(),
            "majorid".to_string(),
            "gradyear".to_string(),
        ];

        let p4 = Arc::new(Mutex::new(ProjectPlan::new(p3, c).unwrap()));
        print_stats(4, p4);
    }

    #[test]
    fn test_multi_table_plan() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new(temp_dir.path());
        let mdm = Arc::new(Mutex::new(db.metadata_manager()));
        let tx = db.new_tx();

        let mut planner = db.planner.unwrap();

        let cmd = "create table student(sname varchar(9), majorid int, gradyear int)";
        planner.execute_update(cmd, tx.clone()).unwrap();

        let n = 200;
        for i in 0..n {
            let a = i;
            let b = format!("bbb{}", a);
            let cmd = format!("insert into student(sname,majorid) values('{}', {})", b, a);
            planner.execute_update(&cmd, tx.clone()).unwrap();
        }

        let cmd = "create table dept(dname varchar(9),did int)";
        planner.execute_update(cmd, tx.clone()).unwrap();

        for i in 0..n {
            let c = i;
            let d = format!("ddd{}", c);
            let cmd = format!("insert into dept(dname,did) values('{}', {})", d, c);
            planner.execute_update(&cmd, tx.clone()).unwrap();
        }

        let p1 = Arc::new(Mutex::new(
            TablePlan::new(tx.clone(), "student".to_string(), mdm.clone()).unwrap(),
        ));
        print_stats(1, p1.clone());

        let p2 = Arc::new(Mutex::new(
            TablePlan::new(tx.clone(), "dept".to_string(), mdm).unwrap(),
        ));
        print_stats(2, p2.clone());

        let p3 = Arc::new(Mutex::new(ProductPlan::new(p1, p2).unwrap()));
        print_stats(3, p3.clone());
        let mut cnt = 0;
        let s = p3.lock().unwrap().open().unwrap();
        while s.lock().unwrap().next().unwrap() {
            let _left = s.lock().unwrap().get_string(&"sname".to_string()).unwrap();
            let _right = s.lock().unwrap().get_string(&"dname".to_string()).unwrap();
            cnt += 1;
        }
        assert!(cnt == n * n);

        let t = Term::new(
            Expression::new_from_fldname("majorid".to_string()),
            Expression::new_from_fldname("did".to_string()),
        );
        let pred = Predicate::new_from_term(t);
        let mut p4 = Arc::new(Mutex::new(SelectPlan::new(p3, pred)));
        print_stats(4, p4.clone());

        let mut cnt = 0;
        let mut s = p4.lock().unwrap().open().unwrap();
        while s.lock().unwrap().next().unwrap() {
            let left = s.lock().unwrap().get_string(&"sname".to_string()).unwrap();
            let right = s.lock().unwrap().get_string(&"dname".to_string()).unwrap();
            assert_eq!(left[3..], right[3..]);
            cnt += 1;
        }
        assert!(cnt == n)
    }

    fn print_stats(n: i32, p: Arc<Mutex<dyn Plan>>) {
        let p = p.lock().unwrap();
        println!("Here are the stats for plan p {}", n);
        println!("\tR(p{}): {}", n, p.records_output().unwrap());
        println!("\tB(p{}): {}\n", n, p.blocks_accessed().unwrap());
    }
}
