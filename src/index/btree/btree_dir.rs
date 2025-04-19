use std::sync::{Arc, Mutex};

use crate::{
    file::block_id::BlockId, query::constant::Constant, record::layout::Layout,
    tx::transaction::Transaction,
};

use super::{btree_page::BTPage, dir_entry::DirEntry};

pub struct BTreeDir {
    tx: Arc<Mutex<Transaction>>,
    layout: Arc<Mutex<Layout>>,
    contents: BTPage,
    filename: String,
}

impl BTreeDir {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        blk: BlockId,
        layout: Arc<Mutex<Layout>>,
    ) -> Result<Self, String> {
        let filename = blk.file_name();
        let contents = BTPage::new(tx.clone(), blk, layout.clone())?;

        Ok(BTreeDir {
            tx,
            layout,
            contents,
            filename,
        })
    }

    pub fn close(&mut self) -> Result<(), String> {
        self.contents.close()
    }

    pub fn search(&mut self, searchkey: &Constant) -> Result<i32, String> {
        let mut childblk = self.find_child_block(searchkey)?;
        while self.contents.get_flag()? > 0 {
            self.contents.close()?;
            self.contents = BTPage::new(self.tx.clone(), childblk, self.layout.clone())?;
            childblk = self.find_child_block(searchkey)?;
        }

        Ok(childblk.number())
    }

    pub fn make_new_root(&mut self, e: DirEntry) -> Result<(), String> {
        let firstval = self.contents.get_data_val(0)?;
        let level = self.contents.get_flag()?;
        let newblk = self.contents.split(0, level)?;
        let oldroot = DirEntry::new(firstval, newblk.number());

        self.insert_entry(oldroot)?;
        self.insert_entry(e)?;
        self.contents.set_flag(level + 1)?;

        Ok(())
    }

    pub fn insert(&mut self, e: DirEntry) -> Result<Option<DirEntry>, String> {
        if self.contents.get_flag()? == 0 {
            return self.insert_entry(e);
        }
        let childblk = self.find_child_block(&e.data_val())?;
        let mut child = BTreeDir::new(self.tx.clone(), childblk, self.layout.clone())?;
        let myentry = child.insert(e)?;
        child.close()?;
        if let Some(entry) = myentry {
            self.insert_entry(entry)
        } else {
            Ok(None)
        }
    }

    fn insert_entry(&mut self, e: DirEntry) -> Result<Option<DirEntry>, String> {
        let newslot = 1 + self.contents.find_slot_before(&e.data_val())?;
        self.contents
            .insert_dir(newslot, e.data_val(), e.block_number())?;
        if self.contents.is_full()? {
            return Ok(None);
        }
        let level = self.contents.get_flag()?;
        let splitpos = self.contents.get_num_recs()? / 2;
        let splitval = self.contents.get_data_val(splitpos)?;
        let newblk = self.contents.split(splitpos, level)?;
        return Ok(Some(DirEntry::new(splitval, newblk.number())));
    }

    fn find_child_block(&self, searchkey: &Constant) -> Result<BlockId, String> {
        let mut slot = self.contents.find_slot_before(searchkey)?;
        if self.contents.get_data_val(slot + 1)?.eq(searchkey) {
            slot += 1;
        }

        let blknum = self.contents.get_child_num(slot)?;

        Ok(BlockId::new(self.filename.clone(), blknum))
    }
}
