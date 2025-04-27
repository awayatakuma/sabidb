use std::sync::{Arc, Mutex};

use crate::{
    metadata::matadata_manager::MetadataManager,
    parse::query_data::QueryData,
    plan::{plan::Plan, project_plan::ProjectPlan, query_planner::QueryPlanner},
};

use super::table_planner::TablePlanner;

pub struct HeuristicQueryPlanner {
    tableplanners: Vec<TablePlanner>,
    mdm: Arc<Mutex<MetadataManager>>,
}

impl HeuristicQueryPlanner {
    pub fn new(mdm: Arc<Mutex<MetadataManager>>) -> Self {
        HeuristicQueryPlanner {
            tableplanners: Vec::new(),
            mdm,
        }
    }

    fn get_lowest_select_plan(&mut self) -> Result<Arc<Mutex<dyn Plan>>, String> {
        let mut best_i = 0;
        let mut bestplan = self.tableplanners[0].make_select_plan()?;
        for (i, tp) in self.tableplanners[1..].iter().enumerate() {
            let plan = tp.make_select_plan()?;
            if plan
                .lock()
                .map_err(|_| "failed to get lock")?
                .records_output()?
                < bestplan
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .records_output()?
            {
                bestplan = plan;
                best_i = i;
            }
        }

        self.tableplanners.remove(best_i);
        Ok(bestplan)
    }

    fn get_lowest_join_plan(
        &mut self,
        current: Arc<Mutex<dyn Plan>>,
    ) -> Result<Option<Arc<Mutex<dyn Plan>>>, String> {
        let mut best_i = 0;
        let mut bestplan: Option<Arc<Mutex<dyn Plan>>> = None;
        for (i, tp) in self.tableplanners.iter().enumerate() {
            if let Some(plan) = tp.make_join_plan(current.clone())? {
                if bestplan.is_none()
                    || plan
                        .lock()
                        .map_err(|_| "failed to get lock")?
                        .records_output()?
                        < bestplan
                            .as_ref()
                            .unwrap()
                            .lock()
                            .map_err(|_| "failed to get lock")?
                            .records_output()?
                {
                    best_i = i;
                    bestplan = Some(plan);
                }
            }
        }
        if bestplan.is_some() {
            self.tableplanners.remove(best_i);
        }

        Ok(bestplan)
    }

    fn get_lowest_product_plan(
        &mut self,
        current: Arc<Mutex<dyn Plan>>,
    ) -> Result<Arc<Mutex<dyn Plan>>, String> {
        let mut best_i = 0;
        let mut bestplan = self.tableplanners[0].make_select_plan()?;
        for (i, tp) in self.tableplanners[1..].iter().enumerate() {
            let plan = tp.make_product_plan(current.clone())?;
            if plan
                .lock()
                .map_err(|_| "failed to get lock")?
                .records_output()?
                < bestplan
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .records_output()?
            {
                bestplan = plan;
                best_i = i;
            }
        }

        self.tableplanners.remove(best_i);
        Ok(bestplan)
    }
}

impl QueryPlanner for HeuristicQueryPlanner {
    fn create_plan(
        &mut self,
        data: QueryData,
        tx: Arc<Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<Arc<Mutex<dyn crate::plan::plan::Plan>>, String> {
        // Step 1:  Create a TablePlanner object for each mentioned table
        for tblname in data.tables() {
            let tp = TablePlanner::new(tblname, data.pred(), tx.clone(), self.mdm.clone())?;
            self.tableplanners.push(tp);
        }

        // Step 2:  Choose the lowest-size plan to begin the join order
        let mut currentplan = self.get_lowest_select_plan()?;

        // Step 3:  Repeatedly add a plan to the join order
        while !self.tableplanners.is_empty() {
            if let Some(p) = self.get_lowest_join_plan(currentplan.clone())? {
                currentplan = p
            } else {
                currentplan = self.get_lowest_product_plan(currentplan)?;
            }
        }

        // Step 4.  Project on the field names and return
        Ok(Arc::new(Mutex::new(ProjectPlan::new(
            currentplan,
            data.fields(),
        )?)))
    }
}

#[cfg(test)]
mod tests {

    use std::sync::{Arc, Mutex};

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
        let db = SimpleDB::new_with_refined_planners(temp_dir.path());
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
        let db = SimpleDB::new_with_refined_planners(temp_dir.path());
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

        let qry = "select A, B, C, D from T,TT where A=C";
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
        let db = SimpleDB::new_with_refined_planners(temp_dir.path());
        let mdm = db.metadata_manager();
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
        let db = SimpleDB::new_with_refined_planners(temp_dir.path());
        let mdm = db.metadata_manager();
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
        let p4 = Arc::new(Mutex::new(SelectPlan::new(p3, pred)));
        print_stats(4, p4.clone());

        let mut cnt = 0;
        let s = p4.lock().unwrap().open().unwrap();
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
