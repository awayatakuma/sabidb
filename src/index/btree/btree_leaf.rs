use std::sync::{Arc, Mutex};

use crate::{
    file::block_id::BlockId,
    query::constant::Constant,
    record::{layout::Layout, rid::RID},
    tx::transaction::Transaction,
};

use super::{btree_page::BTPage, dir_entry::DirEntry};

pub struct BTreeLeaf {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Mutex<Layout>>,
    search_key: Constant,
    contents: BTPage,
    currentslot: i32,
    filename: String,
}

impl BTreeLeaf {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        blk: BlockId,
        layout: Arc<Mutex<Layout>>,
        search_key: Constant,
    ) -> Result<Self, String> {
        let filename = blk.file_name();
        let contents = BTPage::new(tx.clone(), blk, layout.clone())?;
        let currentslot = contents.find_slot_before(&search_key)?;
        Ok(BTreeLeaf {
            tx: tx,
            layout: layout,
            search_key: search_key,
            contents: contents,
            currentslot: currentslot,
            filename: filename,
        })
    }

    pub fn close(&mut self) -> Result<(), String> {
        self.contents.close()
    }

    pub fn next(&mut self) -> Result<bool, String> {
        self.currentslot += 1;
        if self.currentslot >= self.contents.get_num_recs()? {
            self.try_overflow()
        } else if self
            .contents
            .get_data_val(self.currentslot)?
            .eq(&self.search_key)
        {
            Ok(true)
        } else {
            self.try_overflow()
        }
    }

    pub fn get_data_rid(&self) -> Result<RID, String> {
        self.contents.get_data_rid(self.currentslot)
    }

    pub fn delete(&mut self, datarid: RID) -> Result<(), String> {
        while self.next()? {
            if self.get_data_rid()?.eq(&datarid) {
                self.contents.delete(self.currentslot)?;
                return Ok(());
            }
        }
        Ok(())
    }

    pub fn insert(&mut self, datarid: RID) -> Result<Option<DirEntry>, String> {
        if self.contents.get_flag()? >= 0
            && self.contents.get_data_val(0)?.partial_cmp(&self.search_key)
                == Some(std::cmp::Ordering::Greater)
        {
            let firstval = self.contents.get_data_val(0)?;
            let newblk = self.contents.split(0, self.contents.get_flag()?)?;
            self.currentslot = 0;
            self.contents.set_flag(-1)?;
            self.contents
                .insert_leaf(self.currentslot, self.search_key.clone(), &datarid)?;
            return Ok(Some(DirEntry::new(firstval, newblk.number())));
        }

        self.currentslot += 1;
        self.contents
            .insert_leaf(self.currentslot, self.search_key.clone(), &datarid)?;
        if !self.contents.is_full()? {
            return Ok(None);
        }

        let firstkey = self.contents.get_data_val(0)?;
        let lastkey = self
            .contents
            .get_data_val(self.contents.get_num_recs()? - 1)?;
        if lastkey.eq(&firstkey) {
            let newblk = self.contents.split(1, self.contents.get_flag()?)?;
            self.contents.set_flag(newblk.number())?;
            return Ok(None);
        } else {
            let mut splitpos = self.contents.get_num_recs()? / 2;
            let mut splitkey = self.contents.get_data_val(splitpos)?;
            if splitkey.eq(&firstkey) {
                while self.contents.get_data_val(splitpos)?.eq(&splitkey) {
                    splitpos += 1;
                }
                splitkey = self.contents.get_data_val(splitpos)?;
            } else {
                while self.contents.get_data_val(splitpos - 1)?.eq(&splitkey) {
                    splitpos -= 1;
                }
            }
            let newblk = self.contents.split(splitpos, -1)?;
            return Ok(Some(DirEntry::new(splitkey, newblk.number())));
        }
    }

    fn try_overflow(&mut self) -> Result<bool, String> {
        let firstkey = self.contents.get_data_val(0)?;
        let flag = self.contents.get_flag()?;
        if !self.search_key.eq(&firstkey) || flag < 0 {
            return Ok(false);
        }
        self.contents.close()?;
        let nextblk = BlockId::new(self.filename.clone(), flag);
        self.contents = BTPage::new(self.tx.clone(), nextblk, self.layout.clone())?;
        self.currentslot = 0;

        Ok(true)
    }
}
