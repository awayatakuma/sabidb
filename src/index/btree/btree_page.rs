use std::sync::{Arc, Mutex};

use crate::{
    constants::INTEGER_BYTES,
    file::block_id::BlockId,
    query::constant::Constant,
    record::{
        layout::Layout,
        rid::RID,
        schema::field_type::{INTEGER, VARCHAR},
    },
    tx::transaction::Transaction,
};

pub struct BTPage {
    tx: Arc<Mutex<Transaction>>,
    currentblk: Option<BlockId>,
    layout: Arc<Mutex<Layout>>,
}

impl BTPage {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        currentblk: BlockId,
        layout: Arc<Mutex<Layout>>,
    ) -> Result<Self, String> {
        tx.lock()
            .map_err(|_| "failed to get lock")?
            .pin(&currentblk)?;
        Ok(BTPage {
            tx: tx,
            currentblk: Some(currentblk),
            layout: layout,
        })
    }

    pub fn find_slot_before(&self, search_key: &Constant) -> Result<i32, String> {
        let mut slot = 0;
        while slot < self.get_num_recs()?
            && self.get_data_val(slot)?.partial_cmp(search_key) == Some(std::cmp::Ordering::Less)
        {
            slot += 1;
        }

        Ok(slot - 1)
    }

    pub fn close(&mut self) -> Result<(), String> {
        if let Some(blk) = &self.currentblk {
            self.tx
                .lock()
                .map_err(|_| "failed to get lock")?
                .unpin(blk)?;
        }

        self.currentblk = None;
        Ok(())
    }

    pub fn is_full(&self) -> Result<bool, String> {
        Ok(self.slotpos(self.get_num_recs()? + 1)
            > self
                .tx
                .lock()
                .map_err(|_| "failed to get lock")?
                .block_size())
    }

    pub fn split(&self, splitpos: i32, flag: i32) -> Result<BlockId, String> {
        let newblk = self.append_new(flag)?;
        let mut newpage = BTPage::new(self.tx.clone(), newblk.clone(), self.layout.clone())?;
        self.transfer_recs(splitpos, &mut newpage)?;
        newpage.set_flag(flag)?;
        newpage.close()?;
        Ok(newblk)
    }

    pub fn get_data_val(&self, slot: i32) -> Result<Constant, String> {
        self.get_val(slot, "dataval".to_string())
    }

    pub fn get_flag(&self) -> Result<i32, String> {
        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_int(&self.currentblk.clone().unwrap(), 0)
    }

    pub fn set_flag(&self, val: i32) -> Result<(), String> {
        self.tx.lock().map_err(|_| "failed to get lock")?.set_int(
            &self.currentblk.clone().unwrap(),
            0,
            val,
            true,
        )
    }

    pub fn append_new(&self, flag: i32) -> Result<BlockId, String> {
        let tx = self.tx.lock().map_err(|_| "failed to get lock")?;
        let blk = tx.append(self.currentblk.clone().unwrap().file_name())?;
        self.format(&blk, flag)?;
        Ok(blk)
    }

    pub fn format(&self, blk: &BlockId, flag: i32) -> Result<(), String> {
        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .set_int(&blk, 0, flag, false)?;
        self.tx.lock().map_err(|_| "failed to get lock")?.set_int(
            &blk,
            INTEGER_BYTES as usize,
            0,
            false,
        )?;
        let recsize = self
            .layout
            .lock()
            .map_err(|_| "failed to get lock")?
            .slot_size();
        let mut pos = 2 * INTEGER_BYTES;
        while pos + recsize
            <= self
                .tx
                .lock()
                .map_err(|_| "failed to get lock")?
                .block_size()?
        {
            self.make_default_record(blk, pos)?;
            pos += recsize;
        }
        Ok(())
    }

    fn make_default_record(&self, blk: &BlockId, pos: i32) -> Result<(), String> {
        let layout = self.layout.lock().map_err(|_| "failed to get lock")?;
        let binding = layout
            .schema()
            .lock()
            .map_err(|_| "failed to get lock")?
            .fields();
        let binding = binding.lock().map_err(|_| "failed to get lock")?;
        let flds = binding.iter();
        for fldname in flds {
            let offset = layout.offset(&fldname)?;
            if layout
                .schema()
                .lock()
                .map_err(|_| "failed to get lock")?
                .field_type(fldname)?
                == INTEGER
            {
                self.tx.lock().map_err(|_| "failed to get lock")?.set_int(
                    blk,
                    pos as usize + offset,
                    0,
                    false,
                )?;
            } else if layout
                .schema()
                .lock()
                .map_err(|_| "failed to get lock")?
                .field_type(fldname)?
                == VARCHAR
            {
                self.tx
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .set_string(blk, pos as usize + offset, "".to_string(), false)?;
            } else {
                panic!("Unreachable!!")
            }
        }

        Ok(())
    }

    pub fn get_child_num(&self, slot: i32) -> Result<i32, String> {
        self.get_int(slot, "block".to_string())
    }

    pub fn insert_dir(&self, slot: i32, val: Constant, blknum: i32) -> Result<(), String> {
        self.insert(slot)?;
        self.set_val(slot, "dataval".to_string(), val)?;
        self.set_int(slot, "block".to_string(), blknum)?;
        Ok(())
    }

    pub fn get_data_rid(&self, slot: i32) -> Result<RID, String> {
        Ok(RID::new(
            self.get_int(slot, "block".to_string())?,
            self.get_int(slot, "id".to_string())?,
        ))
    }

    pub fn insert_leaf(&self, slot: i32, val: Constant, rid: &RID) -> Result<(), String> {
        self.insert(slot)?;
        self.set_val(slot, "dataval".to_string(), val)?;
        self.set_int(slot, "block".to_string(), rid.block_number())?;
        self.set_int(slot, "id".to_string(), rid.slot())?;

        Ok(())
    }

    pub fn delete(&self, slot: i32) -> Result<(), String> {
        let mut i = slot + 1;
        while i < self.get_num_recs()? {
            self.copy_record(i, i - 1)?;
            i += 1;
        }
        self.set_num_recs(self.get_num_recs()? - 1)?;
        Ok(())
    }

    pub fn get_num_recs(&self) -> Result<i32, String> {
        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_int(self.currentblk.as_ref().unwrap(), INTEGER_BYTES as usize)
    }

    fn get_int(&self, slot: i32, fldname: String) -> Result<i32, String> {
        let pos = self.fldpos(slot, fldname)?;
        return self
            .tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_int(&self.currentblk.as_ref().unwrap(), pos as usize);
    }

    fn get_string(&self, slot: i32, fldname: String) -> Result<String, String> {
        let pos = self.fldpos(slot, fldname)?;
        return self
            .tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_string(self.currentblk.as_ref().unwrap(), pos as usize);
    }

    fn get_val(&self, slot: i32, fldname: String) -> Result<Constant, String> {
        let fldtype = self
            .layout
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()
            .lock()
            .map_err(|_| "failed to get lock")?
            .field_type(&fldname)?;
        if fldtype == INTEGER {
            Ok(Constant::new_from_i32(self.get_int(slot, fldname)?))
        } else if fldtype == VARCHAR {
            Ok(Constant::new_from_string(self.get_string(slot, fldname)?))
        } else {
            panic!("Unreachable!!")
        }
    }

    fn set_int(&self, slot: i32, fldname: String, val: i32) -> Result<(), String> {
        let pos = self.fldpos(slot, fldname)?;
        self.tx.lock().map_err(|_| "failed to get lock")?.set_int(
            &self.currentblk.clone().unwrap(),
            pos as usize,
            val,
            true,
        )
    }

    fn set_string(&self, slot: i32, fldname: String, val: String) -> Result<(), String> {
        let pos = self.fldpos(slot, fldname)?;
        self.tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .set_string(&self.currentblk.clone().unwrap(), pos as usize, val, true)
    }

    fn set_val(&self, slot: i32, fldname: String, val: Constant) -> Result<(), String> {
        let fldtype = self
            .layout
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema()
            .lock()
            .map_err(|_| "failed to get lock")?
            .field_type(&fldname)?;
        if fldtype == INTEGER {
            self.set_int(slot, fldname, val.as_int().unwrap())?;
        } else if fldtype == VARCHAR {
            self.set_string(slot, fldname, val.as_string().unwrap())?;
        } else {
            panic!("Unreachable")
        }

        Ok(())
    }

    fn set_num_recs(&self, n: i32) -> Result<(), String> {
        self.tx.lock().map_err(|_| "failed to get lock")?.set_int(
            &self.currentblk.clone().unwrap(),
            INTEGER_BYTES as usize,
            n,
            true,
        )
    }

    fn insert(&self, slot: i32) -> Result<(), String> {
        let mut i = self.get_num_recs()?;
        while i > slot {
            self.copy_record(i - 1, i)?;
            i = -1;
        }
        self.set_num_recs(self.get_num_recs()? + 1)?;
        Ok(())
    }

    fn copy_record(&self, from: i32, to: i32) -> Result<(), String> {
        let sch = self
            .layout
            .lock()
            .map_err(|_| "failed to get lock")?
            .schema();
        for fldname in sch
            .lock()
            .map_err(|_| "failed to get lock")?
            .fields()
            .lock()
            .map_err(|_| "failed to get lock")?
            .iter()
        {
            self.set_val(to, fldname.clone(), self.get_val(from, fldname.clone())?)?;
        }
        Ok(())
    }

    fn transfer_recs(&self, slot: i32, dest: &mut BTPage) -> Result<(), String> {
        let mut destslot = 0;
        while slot < self.get_num_recs()? {
            dest.insert(destslot)?;
            let sch = self
                .layout
                .lock()
                .map_err(|_| "failed to get lock")?
                .schema();
            for fldname in sch
                .lock()
                .map_err(|_| "failed to get lock")?
                .fields()
                .lock()
                .map_err(|_| "failed to get lock")?
                .iter()
            {
                dest.set_val(
                    destslot,
                    fldname.clone(),
                    self.get_val(slot, fldname.clone())?,
                )?;
            }
            self.delete(slot)?;
            destslot += 1;
        }

        Ok(())
    }

    fn fldpos(&self, slot: i32, fldname: String) -> Result<i32, String> {
        let offset = self
            .layout
            .lock()
            .map_err(|_| "failed to get lock")?
            .offset(&fldname)?;
        let ret = self.slotpos(slot)? + offset as i32;
        Ok(ret)
    }

    fn slotpos(&self, slot: i32) -> Result<i32, String> {
        let slotsize = self
            .layout
            .lock()
            .map_err(|_| "failed to get lock")?
            .slot_size();
        Ok(INTEGER_BYTES + INTEGER_BYTES + (slot * slotsize))
    }
}
