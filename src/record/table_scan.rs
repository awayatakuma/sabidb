use std::sync::{Arc, Mutex};

use crate::{
    file::block_id::BlockId,
    query::{constant::Constant, scan::Scan, update_scan::UpdateScan},
    tx::transaction::Transaction,
};

use super::{layout::Layout, record_page::RecordPage, rid::RID, schema::field_type::INTEGER};

#[derive(Debug, Clone)]
pub struct TableScan {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Mutex<Layout>>,
    rp: Arc<Mutex<RecordPage>>,
    filename: String,
    current_slot: i32,
}

impl TableScan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        tblname: String,
        layout: Arc<Mutex<Layout>>,
    ) -> Result<Self, String> {
        let filename = tblname + ".tbl";
        let blk = if tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .size(filename.clone())?
            == 0
        {
            // moveToNewBlock
            let blk = tx
                .lock()
                .map_err(|_| "failed to get lock")?
                .append(filename.clone())?;
            blk
        } else {
            // moveToBlock
            let blk = BlockId::new(filename.clone(), 0);
            blk
        };
        let rp = RecordPage::new(tx.clone(), blk, layout.clone())?;
        Ok(TableScan {
            tx: tx,
            layout: layout,
            rp: Arc::new(Mutex::new(rp)),
            filename: filename,
            current_slot: -1,
        })
    }

    // Private auxiliary methods
    fn move_to_block(&mut self, blknum: i32) -> Result<(), String> {
        self.close()?;
        let blk = BlockId::new(self.filename.clone(), blknum);
        self.rp = Arc::new(Mutex::new(RecordPage::new(
            self.tx.clone(),
            blk,
            self.layout.clone(),
        )?));
        self.current_slot = -1;

        Ok(())
    }
    fn move_to_new_block(&mut self) -> Result<(), String> {
        self.close()?;
        let blk = self
            .tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .append(self.filename.clone())?;
        self.rp = Arc::new(Mutex::new(RecordPage::new(
            self.tx.clone(),
            blk,
            self.layout.clone(),
        )?));
        self.rp.lock().map_err(|_| "failed to get lock")?.format()?;
        self.current_slot = -1;

        Ok(())
    }
    fn at_last_block(&mut self) -> Result<bool, String> {
        let ret = self
            .rp
            .lock()
            .map_err(|_| "failed to get lock")?
            .block()
            .number()
            == self
                .tx
                .lock()
                .map_err(|_| "failed to get lock")?
                .size(self.filename.clone())?
                - 1;
        Ok(ret)
    }
}

impl Scan for TableScan {
    fn before_first(&mut self) -> Result<(), String> {
        self.move_to_block(0)
    }

    fn next(&mut self) -> Result<bool, String> {
        self.current_slot = self
            .rp
            .lock()
            .map_err(|_| "failed to get lock")?
            .next_after(self.current_slot)?;
        while self.current_slot < 0 {
            if self.at_last_block()? {
                return Ok(false);
            }
            let blknum = self
                .rp
                .lock()
                .map_err(|_| "failed to get lock")?
                .block()
                .number()
                + 1;
            self.move_to_block(blknum)?;
            self.current_slot = self
                .rp
                .lock()
                .map_err(|_| "failed to get lock")?
                .next_after(self.current_slot)?;
        }
        Ok(true)
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        let ret = self
            .rp
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_int(self.current_slot, fldname.clone())?;
        Ok(ret)
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        let ret = self
            .rp
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_string(self.current_slot, fldname.clone())?;
        Ok(ret)
    }

    fn get_val(&self, fldname: &String) -> Result<Constant, String> {
        let ret = if self
            .layout
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()
            .lock()
            .map_err(|_| "failed to get lock")?
            .field_type(fldname)?
            == INTEGER
        {
            Constant::mew_from_i32(self.get_int(fldname)?)
        } else {
            Constant::mew_from_string(self.get_string(fldname)?)
        };
        Ok(ret)
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        let ret = self
            .layout
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)?;
        Ok(ret)
    }

    fn close(&mut self) -> Result<(), String> {
        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .unpin(&self.rp.lock().map_err(|_| "failed to get lock")?.block())?;
        Ok(())
    }
}

impl UpdateScan for TableScan {
    fn set_val(
        &mut self,
        fldname: String,
        val: crate::query::constant::Constant,
    ) -> Result<(), String> {
        if self
            .layout
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()
            .lock()
            .map_err(|_| "failed to get lock")?
            .field_type(&fldname)?
            == INTEGER
        {
            self.set_int(fldname, val.as_int().unwrap())?;
        } else {
            self.set_string(fldname, val.as_string().unwrap())?;
        }
        Ok(())
    }

    fn set_int(&mut self, fldname: String, val: i32) -> Result<(), String> {
        self.rp
            .lock()
            .map_err(|_| "failed to get lock")?
            .set_int(self.current_slot, fldname, val)
    }

    fn set_string(&mut self, fldname: String, val: String) -> Result<(), String> {
        self.rp
            .lock()
            .map_err(|_| "failed to get lock")?
            .set_string(self.current_slot, fldname, val)
    }

    fn insert(&mut self) -> Result<(), String> {
        self.current_slot = self
            .rp
            .lock()
            .map_err(|_| "failed to get lock")?
            .insert_after(self.current_slot)?;
        while self.current_slot < 0 {
            if self.at_last_block()? {
                self.move_to_new_block()?;
            } else {
                let blknum = self
                    .rp
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .block()
                    .number()
                    + 1;
                self.move_to_block(blknum)?;
            }
            self.current_slot = self
                .rp
                .lock()
                .map_err(|_| "failed to get lock")?
                .insert_after(self.current_slot)?;
        }
        Ok(())
    }

    fn delete(&mut self) -> Result<(), String> {
        self.rp
            .lock()
            .map_err(|_| "failed to get lock")?
            .delete(self.current_slot)?;
        Ok(())
    }

    fn get_rid(&mut self) -> Result<RID, String> {
        let ret = RID::new(
            self.rp
                .lock()
                .map_err(|_| "failed to get lock")?
                .block()
                .number(),
            self.current_slot,
        );

        Ok(ret)
    }

    fn move_to_rid(&mut self, rid: RID) -> Result<(), String> {
        self.close()?;
        let blk = BlockId::new(self.filename.clone(), rid.block_number());
        self.rp = Arc::new(Mutex::new(RecordPage::new(
            self.tx.clone(),
            blk,
            self.layout.clone(),
        )?));
        self.current_slot = rid.slot();

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::sync::{Arc, Mutex};

    use tempfile::TempDir;

    use crate::{
        query::{scan::Scan, update_scan::UpdateScan},
        record::{schema::Schema, table_scan::TableScan},
        server::simple_db::SimpleDB,
    };

    use super::Layout;

    use rand::prelude::*;

    #[test]
    fn test_table_scan() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(SimpleDB::new_with_sizes(temp_dir.path(), 400, 8));
        let tx = db.new_tx();

        let mut sch = Schema::new();
        sch.add_int_field(&"A".to_string()).unwrap();
        sch.add_string_field(&"B".to_string(), 9).unwrap();
        let layout = Arc::new(Mutex::new(
            Layout::new_from_schema(Arc::new(Mutex::new(sch))).unwrap(),
        ));

        {
            let binding = layout.lock().unwrap().schema().lock().unwrap().fields();
            let binding = binding.lock().unwrap();
            let fldnames = binding.iter();

            for fldname in fldnames {
                let offset = layout.lock().unwrap().offset(fldname).unwrap();
                if fldname == "A" {
                    assert_eq!(offset, 4)
                } else if fldname == "B" {
                    assert_eq!(offset, 8)
                } else {
                    panic!("unreachable!!")
                }
            }
        }

        println!("Filling the table with 50 random records");
        let mut ts = TableScan::new(tx.clone(), "T".to_string(), layout).unwrap();
        let mut rng = rand::rng();
        for _ in 0..50 {
            ts.insert().unwrap();
            let n = rng.random_range(0..=50);
            ts.set_int("A".to_string(), n).unwrap();
            ts.set_string("B".to_string(), format!("rec{}", n)).unwrap();
            println!(
                "inserting into slot {} : [{}, rec{}]",
                ts.get_rid().unwrap(),
                n,
                n
            )
        }

        println!("Deleting these records, whose A-values are less than 25 ");

        ts.before_first().unwrap();
        while ts.next().unwrap() {
            let a = ts.get_int(&"A".to_string()).unwrap();
            let b = ts.get_string(&"B".to_string()).unwrap();
            if a < 25 {
                println!("slot {} : [{}, {}]", ts.get_rid().unwrap(), a, b);
                ts.delete().unwrap();
            }
        }

        println!("under 25 were deleted");

        println!("Here are the remaining records.");
        ts.before_first().unwrap();
        while ts.next().unwrap() {
            let a = ts.get_int(&"A".to_string()).unwrap();
            let b = ts.get_string(&"B".to_string()).unwrap();
            assert!(a >= 25);
            println!("slot {} : [{}, {}]", ts.get_rid().unwrap(), a, b);
        }
        ts.close().unwrap();
        tx.lock().unwrap().commit().unwrap();
    }
}
