use std::sync::{Arc, Mutex};

use super::{scan::Scan, update_scan::UpdateScan};

pub struct ProductScan {
    s1: Arc<Mutex<dyn Scan>>,
    s2: Arc<Mutex<dyn Scan>>,
}

impl ProductScan {
    pub fn new(s1: Arc<Mutex<dyn Scan>>, s2: Arc<Mutex<dyn Scan>>) -> Result<Self, String> {
        let mut ps = ProductScan { s1: s1, s2: s2 };
        ps.before_first()?;
        Ok(ps)
    }
}

impl Scan for ProductScan {
    fn before_first(&mut self) -> Result<(), String> {
        self.s1
            .lock()
            .map_err(|_| "failed to get lock")?
            .before_first()?;
        self.s1.lock().map_err(|_| "failed to get lock")?.next()?;
        self.s2
            .lock()
            .map_err(|_| "failed to get lock")?
            .before_first()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool, String> {
        if self.s2.lock().map_err(|_| "failed to get lock")?.next()? {
            return Ok(true);
        } else {
            self.s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .before_first()?;
            return Ok(self.s2.lock().map_err(|_| "failed to get lock")?.next()?
                && self.s1.lock().map_err(|_| "failed to get lock")?.next()?);
        }
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        if self
            .s1
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?
        {
            return self
                .s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_int(fldname);
        } else {
            return self
                .s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_int(fldname);
        }
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        if self
            .s1
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?
        {
            return self
                .s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_string(fldname);
        } else {
            return self
                .s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_string(fldname);
        }
    }

    fn get_val(&self, fldname: &String) -> Result<super::constant::Constant, String> {
        if self
            .s1
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?
        {
            return self
                .s1
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(fldname);
        } else {
            return self
                .s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_val(fldname);
        }
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        Ok(self
            .s1
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?
            || self
                .s2
                .lock()
                .map_err(|_| "failed to get lock")?
                .has_field(fldname)?)
    }

    fn close(&mut self) -> Result<(), String> {
        self.s1.lock().map_err(|_| "failed to get lock")?.close()?;
        self.s2.lock().map_err(|_| "failed to get lock")?.close()?;
        Ok(())
    }

    fn to_update_scan(&mut self) -> Result<Arc<Mutex<(dyn UpdateScan + 'static)>>, String> {
        Err("Unexpected downcast".to_string())
    }

    fn as_table_scan(&mut self) -> Result<&mut crate::record::table_scan::TableScan, String> {
        Err("Unexpected downcast".to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::TempDir;

    use crate::{
        query::{product_scan::ProductScan, scan::Scan, update_scan::UpdateScan},
        record::{layout::Layout, schema::Schema, table_scan::TableScan},
        server::simple_db::SimpleDB,
    };

    #[test]
    fn test_product_scan() {
        let temp_dir = TempDir::new().unwrap();
        let db = SimpleDB::new(temp_dir.path());

        let tx = db.new_tx();

        let mut sch1 = Schema::new();
        sch1.add_int_field(&"A".to_string()).unwrap();
        sch1.add_string_field(&"B".to_string(), 9).unwrap();
        let layout1 = Arc::new(Mutex::new(
            Layout::new_from_schema(Arc::new(Mutex::new(sch1))).unwrap(),
        ));
        let mut ts1 = TableScan::new(tx.clone(), "T1".to_string(), layout1.clone()).unwrap();

        let mut sch2 = Schema::new();
        sch2.add_int_field(&"C".to_string()).unwrap();
        sch2.add_string_field(&"D".to_string(), 9).unwrap();
        let layout2 = Arc::new(Mutex::new(
            Layout::new_from_schema(Arc::new(Mutex::new(sch2))).unwrap(),
        ));
        let mut ts2 = TableScan::new(tx.clone(), "T2".to_string(), layout2.clone()).unwrap();

        let n = 200;

        ts1.before_first().unwrap();
        println!("inserting {} records into T1", n);
        for i in 0..n {
            ts1.insert().unwrap();
            ts1.set_int("A".to_string(), i).unwrap();
            ts1.set_string("B".to_string(), format!("aaa{}", i))
                .unwrap();
        }
        ts1.close().unwrap();

        ts2.before_first().unwrap();
        println!("inserting {} records into T2", n);
        for i in 0..n {
            ts2.insert().unwrap();
            ts2.set_int("C".to_string(), n - i - 1).unwrap();
            ts2.set_string("D".to_string(), format!("bbb{}", n - i - 1))
                .unwrap();
        }
        ts2.close().unwrap();

        let s1 = Arc::new(Mutex::new(
            TableScan::new(tx.clone(), "T1".to_string(), layout1).unwrap(),
        ));
        let s2 = Arc::new(Mutex::new(
            TableScan::new(tx.clone(), "T2".to_string(), layout2).unwrap(),
        ));

        let mut s3 = ProductScan::new(s1, s2).unwrap();
        let mut count = 0;
        while s3.next().unwrap() {
            let md = count / 200;
            let expected = format!("aaa{}", md);
            let actual = s3.get_string(&"B".to_string()).unwrap();
            println!("{}", actual);
            assert_eq!(expected, actual);
            count += 1;
        }
        s3.close().unwrap();
        tx.lock().unwrap().commit().unwrap();

        assert_eq!(count, 40000)
    }
}
