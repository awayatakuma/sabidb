use std::sync::{Arc, Mutex};

use crate::{
    materialize::{materialize_plan::MaterializePlan, temp_table::TempTable},
    plan::plan::Plan,
    record::schema::Schema,
    tx::transaction::Transaction,
};

use super::multibuffer_product_scan::MultibufferProductScan;

pub struct MultibufferProductPlan {
    tx: Arc<Mutex<Transaction>>,
    lhs: Arc<Mutex<dyn Plan>>,
    rhs: Arc<Mutex<dyn Plan>>,
    sch: Schema,
}

impl MultibufferProductPlan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        lhs: Arc<Mutex<dyn Plan>>,
        rhs: Arc<Mutex<dyn Plan>>,
    ) -> Result<Self, String> {
        let lhs = MaterializePlan::new(lhs.clone(), tx.clone());
        let mut sch = Schema::new();
        sch.add_all(Arc::new(Mutex::new(lhs.schema()?)))?;
        sch.add_all(Arc::new(Mutex::new(
            rhs.lock().map_err(|_| "failed to get lock")?.schema()?,
        )))?;

        Ok(MultibufferProductPlan {
            tx,
            lhs: Arc::new(Mutex::new(lhs)),
            rhs,
            sch,
        })
    }

    pub fn copy_records_from(&self, p: Arc<Mutex<dyn Plan>>) -> Result<TempTable, String> {
        let src = p.lock().map_err(|_| "failed to get lock")?.open()?;
        let sch = Arc::new(Mutex::new(
            p.lock().map_err(|_| "failed to get lock")?.schema()?,
        ));
        let t = TempTable::new(self.tx.clone(), sch.clone())?;
        let dest = t
            .open()?
            .lock()
            .map_err(|_| "failed to get lock")?
            .to_update_scan()?;
        while src.lock().map_err(|_| "failed to get lock")?.next()? {
            dest.lock().map_err(|_| "failed to get lock")?.insert()?;
            let flds = sch
                .lock()
                .map_err(|_| "failed to get lock")?
                .fields()
                .lock()
                .map_err(|_| "failed to get lock")?
                .clone();
            for fldname in flds {
                dest.lock().map_err(|_| "failed to get lock")?.set_val(
                    fldname.clone(),
                    src.lock()
                        .map_err(|_| "failed to get lock")?
                        .get_val(&fldname)?,
                )?;
            }
        }

        src.lock().map_err(|_| "failed to get lock")?.close()?;
        dest.lock().map_err(|_| "failed to get lock")?.close()?;

        Ok(t)
    }
}

impl Plan for MultibufferProductPlan {
    fn open(&self) -> Result<Arc<Mutex<dyn crate::query::scan::Scan>>, String> {
        let leftscan = self.lhs.lock().map_err(|_| "failed to get lock")?.open()?;
        let tt = self.copy_records_from(self.rhs.clone())?;
        Ok(Arc::new(Mutex::new(MultibufferProductScan::new(
            self.tx.clone(),
            leftscan,
            tt.table_name(),
            tt.get_layout(),
        )?)))
    }

    fn blocks_accessed(&self) -> Result<i32, String> {
        let avail = self
            .tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .available_buffers()?;
        let size = MaterializePlan::new(self.rhs.clone(), self.tx.clone()).blocks_accessed()?;
        let numchunck = size / avail;

        Ok(self
            .rhs
            .lock()
            .map_err(|_| "failed to get lock")?
            .blocks_accessed()?
            + self
                .lhs
                .lock()
                .map_err(|_| "failed to get lock")?
                .blocks_accessed()?
                * numchunck)
    }

    fn records_output(&self) -> Result<i32, String> {
        Ok(self
            .lhs
            .lock()
            .map_err(|_| "failed to get lock")?
            .records_output()?
            * self
                .rhs
                .lock()
                .map_err(|_| "failed to get lock")?
                .records_output()?)
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        if self
            .lhs
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()?
            .has_field(&fldname)?
        {
            self.lhs
                .lock()
                .map_err(|_| "failed to get lock")?
                .distinct_values(fldname)
        } else {
            self.rhs
                .lock()
                .map_err(|_| "failed to get lock")?
                .distinct_values(fldname)
        }
    }

    fn schema(&self) -> Result<Schema, String> {
        Ok(self.sch.clone())
    }
}
