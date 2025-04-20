use std::sync::{Arc, Mutex};

use crate::{
    file::block_id::BlockId,
    query::{constant::Constant, scan::Scan},
    record::{layout::Layout, record_page::RecordPage, schema::field_type::INTEGER},
    tx::transaction::Transaction,
};

pub struct ChunkScan {
    buffs: Vec<Arc<RecordPage>>,
    tx: Arc<Mutex<Transaction>>,
    filename: String,
    layout: Arc<Mutex<Layout>>,
    startbnum: i32,
    endbnum: i32,
    currentbnum: i32,
    rp: Option<Arc<RecordPage>>,
    currentslot: i32,
}

impl ChunkScan {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        filename: String,
        layout: Arc<Mutex<Layout>>,
        startbnum: i32,
        endbnum: i32,
    ) -> Result<Self, String> {
        let mut buffs = vec![];
        for i in startbnum..=endbnum {
            let blk = BlockId::new(filename.clone(), i);
            buffs.push(Arc::new(RecordPage::new(tx.clone(), blk, layout.clone())?));
        }
        let mut ret = ChunkScan {
            buffs: buffs,
            tx,
            filename,
            layout,
            startbnum,
            endbnum,
            currentbnum: 0,
            rp: None,
            currentslot: 0,
        };
        ret.move_to_block(startbnum);
        Ok(ret)
    }
    fn move_to_block(&mut self, blknum: i32) {
        self.currentbnum = blknum;
        self.rp = self
            .buffs
            .get((self.currentbnum - self.startbnum) as usize)
            .cloned();
        self.currentslot = -1;
    }
}

impl Scan for ChunkScan {
    fn before_first(&mut self) -> Result<(), String> {
        self.move_to_block(self.startbnum);
        Ok(())
    }

    fn next(&mut self) -> Result<bool, String> {
        self.currentslot = self.rp.as_ref().unwrap().next_after(self.currentslot)?;
        while self.currentslot < 0 {
            if self.currentbnum == self.endbnum {
                return Ok(false);
            }
            self.move_to_block(self.rp.as_ref().unwrap().block().number() + 1);
            self.currentslot = self.rp.as_ref().unwrap().next_after(self.currentslot)?;
        }

        Ok(true)
    }

    fn get_int(&self, fldname: &String) -> Result<i32, String> {
        self.rp
            .as_ref()
            .unwrap()
            .get_int(self.currentslot, fldname.clone())
    }

    fn get_string(&self, fldname: &String) -> Result<String, String> {
        self.rp
            .as_ref()
            .unwrap()
            .get_string(self.currentslot, fldname.clone())
    }

    fn get_val(&self, fldname: &String) -> Result<crate::query::constant::Constant, String> {
        if self
            .layout
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()
            .lock()
            .map_err(|_| "failed to get lock")?
            .field_type(fldname)?
            == INTEGER
        {
            return Ok(Constant::new_from_i32(self.get_int(fldname)?));
        } else {
            return Ok(Constant::new_from_string(self.get_string(fldname)?));
        }
    }

    fn has_field(&self, fldname: &String) -> Result<bool, String> {
        self.layout
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()
            .lock()
            .map_err(|_| "failed to get lock")?
            .has_field(fldname)
    }

    fn close(&mut self) -> Result<(), String> {
        for i in 0..self.buffs.len() {
            let blk = BlockId::new(self.filename.clone(), self.startbnum + i as i32);
            self.tx
                .lock()
                .map_err(|_| "failed to get lock")?
                .unpin(&blk)?;
        }

        Ok(())
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
