use std::sync::{Arc, Mutex};

use crate::{plan::plan::Plan, record::layout::Layout, tx::transaction::Transaction};

use super::temp_table::TempTable;

pub struct MaterializePlan {
    srcplan: Arc<Mutex<dyn Plan>>,
    tx: Arc<Mutex<Transaction>>,
}

impl MaterializePlan {
    pub fn new(srcplan: Arc<Mutex<dyn Plan>>, tx: Arc<Mutex<Transaction>>) -> Self {
        MaterializePlan { srcplan, tx }
    }
}

impl Plan for MaterializePlan {
    fn open(&self) -> Result<Arc<Mutex<dyn crate::query::scan::Scan>>, String> {
        let sch = Arc::new(Mutex::new(
            self.srcplan
                .lock()
                .map_err(|_| "failed to get lock")?
                .schema()?,
        ));
        let temp = TempTable::new(self.tx.clone(), sch.clone())?;
        let src = self
            .srcplan
            .lock()
            .map_err(|_| "failed to get lock")?
            .open()?;
        let dest = temp.open()?;
        while src.lock().map_err(|_| "failed to get lock")?.next()? {
            dest.lock().map_err(|_| "failed to get lock")?.insert()?;
            for fldname in sch
                .lock()
                .map_err(|_| "failed to get lock")?
                .fields()
                .lock()
                .map_err(|_| "failed to get lock")?
                .iter()
            {
                dest.lock().map_err(|_| "failed to get lock")?.set_val(
                    fldname.clone(),
                    src.lock()
                        .map_err(|_| "failed to get lock")?
                        .get_val(fldname)?,
                );
            }
        }

        src.lock().map_err(|_| "failed to get lock")?.close()?;
        dest.lock()
            .map_err(|_| "failed to get lock")?
            .before_first()?;

        let res = dest.lock().map_err(|_| "failed to get lock")?.to_scan()?;

        Ok(res)
    }

    fn blocks_accessed(&self) -> Result<i32, String> {
        let layout = Layout::new_from_schema(Arc::new(Mutex::new(
            self.srcplan
                .lock()
                .map_err(|_| "failed to get lock")?
                .schema()?,
        )))?;
        let rpb = self
            .tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .block_size()? as f32
            / layout.slot_size() as f32;

        Ok((self
            .srcplan
            .lock()
            .map_err(|_| "failed to get lock")?
            .records_output()? as f32
            / rpb) as i32)
    }

    fn records_output(&self) -> Result<i32, String> {
        self.srcplan
            .lock()
            .map_err(|_| "failed to get lock")?
            .records_output()
    }

    fn distinct_values(&self, fldname: String) -> Result<i32, String> {
        self.srcplan
            .lock()
            .map_err(|_| "failed to get lock")?
            .distinct_values(fldname)
    }

    fn schema(&self) -> Result<crate::record::schema::Schema, String> {
        self.srcplan
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()
    }
}
