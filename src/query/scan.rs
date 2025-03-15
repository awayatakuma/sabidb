use super::constant::Constant;

pub trait Scan {
    fn before_first(&mut self) -> Result<(), String>;
    fn next(&mut self) -> Result<bool, String>;
    fn get_int(&self, fldname: &String) -> Result<i32, String>;
    fn get_string(&self, fldname: &String) -> Result<String, String>;
    fn get_val(&self, fldname: &String) -> Result<Constant, String>;
    fn has_field(&self, fldname: &String) -> Result<bool, String>;
    fn close(&mut self) -> Result<(), String>;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rand::Rng;
    use tempfile::TempDir;

    use crate::{
        query::{
            constant::Constant, expression::Expression, predicate::Predicate,
            product_scan::ProductScan, project_scan::ProjectScan, scan::Scan,
            select_scan::SelectScan, term::Term, update_scan::UpdateScan,
        },
        record::{layout::Layout, schema::Schema, table_scan::TableScan},
        server::simple_db::SimpleDB,
    };

    #[test]
    fn test_scan_1() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new(temp_dir.path());
        let mut rng = rand::rng();

        let tx = db.new_tx();

        let mut sch1 = Schema::new();
        sch1.add_int_field(&"A".to_string()).unwrap();
        sch1.add_string_field(&"B".to_string(), 9).unwrap();
        let layout1 = Arc::new(Mutex::new(
            Layout::new_from_schema(Arc::new(Mutex::new(sch1))).unwrap(),
        ));
        let mut us1 = TableScan::new(tx.clone(), "T".to_string(), layout1.clone()).unwrap();

        let n = 200;
        us1.before_first().unwrap();
        println!("inserting {} records into T1", n);
        let mut cnt1 = 0;
        for _ in 0..n {
            let i = rng.random_range(0..=50);
            if i == 10 {
                cnt1 += 1;
            }
            us1.insert().unwrap();
            us1.set_int("A".to_string(), i).unwrap();
            us1.set_string("B".to_string(), format!("rec{}", i))
                .unwrap();
        }
        us1.close().unwrap();

        let s2 = TableScan::new(tx.clone(), "T".to_string(), layout1).unwrap();

        let c = Constant::mew_from_i32(10);
        let t = Term::new(
            Expression::new_from_fldname("A".to_string()),
            Expression::new_from_val(c),
        );
        let pred = Predicate::new_from_term(t);
        println!("The predicate is {}", pred);

        let s3 = Box::new(SelectScan::new(s2, pred));

        let mut s4 = ProjectScan::new(s3, vec!["B".to_string()]);

        let mut cnt2 = 0;
        while s4.next().unwrap() {
            let expected = s4.get_string(&"B".to_string()).unwrap();
            println!("{}", expected);
            assert_eq!(expected, "rec10");
            cnt2 += 1;
        }
        s4.close().unwrap();
        tx.lock().unwrap().commit().unwrap();
        assert_eq!(cnt1, cnt2)
    }

    #[test]
    fn test_scan_2() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new(temp_dir.path());

        let tx = db.new_tx();

        let mut sch1 = Schema::new();
        sch1.add_int_field(&"A".to_string()).unwrap();
        sch1.add_string_field(&"B".to_string(), 9).unwrap();
        let layout1 = Arc::new(Mutex::new(
            Layout::new_from_schema(Arc::new(Mutex::new(sch1))).unwrap(),
        ));
        let mut us1 = TableScan::new(tx.clone(), "T1".to_string(), layout1.clone()).unwrap();

        let n = 200;
        us1.before_first().unwrap();
        println!("inserting {} records into T1", n);
        for i in 0..n {
            us1.insert().unwrap();
            us1.set_int("A".to_string(), i).unwrap();
            us1.set_string("B".to_string(), format!("bbb{}", i))
                .unwrap();
        }
        us1.close().unwrap();

        let mut sch2 = Schema::new();
        sch2.add_int_field(&"C".to_string()).unwrap();
        sch2.add_string_field(&"D".to_string(), 9).unwrap();
        let layout2 = Arc::new(Mutex::new(
            Layout::new_from_schema(Arc::new(Mutex::new(sch2))).unwrap(),
        ));
        let mut us2 = TableScan::new(tx.clone(), "T2".to_string(), layout2.clone()).unwrap();

        us2.before_first().unwrap();
        println!("inserting {} records into T2", n);
        for i in 0..n {
            us2.insert().unwrap();
            us2.set_int("C".to_string(), n - i - 1).unwrap();
            us2.set_string("D".to_string(), format!("bbb{}", n - i - 1))
                .unwrap();
        }
        us2.close().unwrap();

        let s1 = Box::new(TableScan::new(tx.clone(), "T1".to_string(), layout1).unwrap());
        let s2 = Box::new(TableScan::new(tx.clone(), "T1".to_string(), layout2).unwrap());

        let s3 = ProductScan::new(s1, s2).unwrap();

        let t = Term::new(
            Expression::new_from_fldname("A".to_string()),
            Expression::new_from_fldname("C".to_string()),
        );
        let pred = Predicate::new_from_term(t);
        println!("The predicate is {}", pred);
        let s4 = Box::new(SelectScan::new(s3, pred));

        let mut s5 = ProjectScan::new(s4, vec!["B".to_string(), "D".to_string()]);
        while s5.next().unwrap() {
            let expected_a = s5.get_string(&"B".to_string()).unwrap();
            let expected_b = s5.get_string(&"D".to_string()).unwrap();
            let expected = format!("{} {}", expected_a, expected_b);
            println!("{}", expected);
            assert_eq!(expected_a, expected_b)
        }
        s5.close().unwrap();
        tx.lock().unwrap().commit().unwrap();
    }
}
