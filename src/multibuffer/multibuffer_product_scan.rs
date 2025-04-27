use std::sync::{Arc, Mutex};

use crate::{
    query::{product_scan::ProductScan, scan::Scan},
    record::layout::Layout,
    tx::transaction::Transaction,
};

use super::{buffer_needs, chunk_scan::ChunkScan};

pub struct MultibufferProductScan {
    tx: Arc<Mutex<Transaction>>,
    lhsscan: Arc<Mutex<dyn Scan>>,
    rhsscan: Option<Arc<Mutex<dyn Scan>>>,
    prodscan: Option<Arc<Mutex<dyn Scan>>>,
    filename: String,
    layout: Arc<Mutex<Layout>>,
    chunksize: i32,
    nextblknum: i32,
    filesize: i32,
}

impl MultibufferProductScan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        lhsscan: Arc<Mutex<dyn Scan>>,
        tblname: String,
        layout: Arc<Mutex<Layout>>,
    ) -> Result<Self, String> {
        let filename = format!("{}.tbl", tblname);
        let filesize = tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .size(filename.clone())?;
        let available = tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .available_buffers()?;
        let chunksize = buffer_needs::best_factor(available, filesize);
        let mut ret = MultibufferProductScan {
            tx,
            lhsscan,
            rhsscan: None,
            prodscan: None,
            filename: filename,
            layout,
            chunksize,
            nextblknum: 0,
            filesize,
        };

        ret.before_first()?;

        Ok(ret)
    }

    fn use_next_chunk(&mut self) -> Result<bool, String> {
        if self.nextblknum >= self.filesize {
            return Ok(false);
        }
        if let Some(rhsscan) = &self.rhsscan {
            rhsscan.lock().map_err(|_| "failed to get lock")?.close()?;
        }
        let mut end = self.nextblknum + self.chunksize - 1;
        if end >= self.filesize {
            end = self.filesize - 1;
        }
        let rhsscan = Arc::new(Mutex::new(ChunkScan::new(
            self.tx.clone(),
            self.filename.clone(),
            self.layout.clone(),
            self.nextblknum,
            end,
        )?));
        self.lhsscan
            .lock()
            .map_err(|_| "failed to get lock")?
            .before_first()?;
        self.prodscan = Some(Arc::new(Mutex::new(ProductScan::new(
            self.lhsscan.clone(),
            rhsscan.clone(),
        )?)));
        self.rhsscan = Some(rhsscan);
        self.nextblknum = end + 1;

        Ok(true)
    }
}

impl Scan for MultibufferProductScan {
    fn before_first(&mut self) -> Result<(), String> {
        self.nextblknum = 0;
        self.use_next_chunk()?;

        Ok(())
    }

    fn next(&mut self) -> Result<bool, String> {
        while !self
            .prodscan
            .as_ref()
            .unwrap()
            .lock()
            .map_err(|_| "failed to get lock")?
            .next()?
        {
            if !self.use_next_chunk()? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        self.prodscan
            .as_ref()
            .unwrap()
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_int(fldname)
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        self.prodscan
            .as_ref()
            .unwrap()
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_string(fldname)
    }

    fn get_val(&self, fldname: &String) -> Result<crate::query::constant::Constant, String> {
        self.prodscan
            .as_ref()
            .unwrap()
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_val(fldname)
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        self.prodscan
            .as_ref()
            .unwrap()
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)
    }

    fn close(&mut self) -> Result<(), String> {
        self.prodscan
            .as_ref()
            .unwrap()
            .lock()
            .map_err(|_| "failed to get lock")?
            .close()
    }

    fn to_update_scan(
        &mut self,
    ) -> Result<Arc<Mutex<dyn crate::query::update_scan::UpdateScan>>, String> {
        todo!()
    }

    fn as_table_scan(&mut self) -> Result<&mut crate::record::table_scan::TableScan, String> {
        todo!()
    }

    fn as_sort_scan(
        &mut self,
    ) -> Result<Arc<Mutex<crate::materialize::sort_scan::SortScan>>, String> {
        todo!()
    }
}
