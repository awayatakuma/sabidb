use std::{
    i32,
    sync::{Arc, Mutex},
};

use crate::{
    file::block_id::BlockId,
    index::index::Index,
    query::constant::Constant,
    record::{
        layout::Layout,
        schema::{
            field_type::{INTEGER, VARCHAR},
            Schema,
        },
    },
    tx::transaction::Transaction,
};

use super::{btree_dir::BTreeDir, btree_leaf::BTreeLeaf, btree_page::BTPage};

pub struct BTreeIndex {
    tx: Arc<Mutex<Transaction>>,
    dir_layout: Arc<Mutex<Layout>>,
    leaf_layout: Arc<Mutex<Layout>>,
    leaftbl: String,
    leaf: Option<BTreeLeaf>,
    rootblk: BlockId,
}

impl BTreeIndex {
    pub fn new(
        tx: Arc<Mutex<Transaction>>,
        idxname: String,
        leaf_layout: Arc<Mutex<Layout>>,
    ) -> Result<Self, String> {
        let leaftbl = format!("{}leaf", idxname);
        let leaftblsize = tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .size(leaftbl.clone())?;
        if leaftblsize == 0 {
            let blk = tx
                .lock()
                .map_err(|_| "failed to get lock")?
                .append(leaftbl.clone())?;
            let node = BTPage::new(tx.clone(), blk.clone(), leaf_layout.clone())?;
            node.format(&blk, -1)?;
        }

        let mut dirsch = Schema::new();
        dirsch.add(
            &"block".to_string(),
            leaf_layout
                .lock()
                .map_err(|_| "failed to get lock")?
                .schema(),
        )?;

        dirsch.add(
            &"dataval".to_string(),
            leaf_layout
                .lock()
                .map_err(|_| "failed to get lock")?
                .schema(),
        )?;

        let dirsch = Arc::new(Mutex::new(dirsch));

        let dirtbl = format!("{}dir", idxname);
        let dir_layout = Arc::new(Mutex::new(Layout::new_from_schema(dirsch.clone())?));

        let rootblk = BlockId::new(dirtbl.clone(), 0);
        if tx
            .lock()
            .map_err(|_| "failed to get lock")?
            .size(dirtbl.clone())?
            == 0
        {
            tx.lock()
                .map_err(|_| "failed to get lock")?
                .append(dirtbl)?;
            let mut node = BTPage::new(tx.clone(), rootblk.clone(), dir_layout.clone())?;
            node.format(&rootblk, 0)?;
            let fldtype = dirsch
                .lock()
                .map_err(|_| "failed to get lock")?
                .field_type(&"dataval".to_string())?;
            let minval = if fldtype == INTEGER {
                Constant::new_from_i32(i32::MIN)
            } else if fldtype == VARCHAR {
                Constant::new_from_string("".to_string())
            } else {
                panic!("Unreachable")
            };
            node.insert_dir(0, minval, 0)?;
            node.close()?;
        }

        Ok(BTreeIndex {
            tx,
            dir_layout,
            leaf_layout,
            leaftbl,
            leaf: None,
            rootblk: rootblk,
        })
    }
}

impl Index for BTreeIndex {
    fn before_first(&mut self, searchkey: &Constant) -> Result<(), String> {
        self.close()?;
        let mut root = BTreeDir::new(
            self.tx.clone(),
            self.rootblk.clone(),
            self.dir_layout.clone(),
        )?;
        let blknum = root.search(searchkey)?;
        root.close()?;
        let leafblk = BlockId::new(self.leaftbl.clone(), blknum);
        self.leaf = Some(BTreeLeaf::new(
            self.tx.clone(),
            leafblk,
            self.leaf_layout.clone(),
            searchkey.clone(),
        )?);
        Ok(())
    }

    fn next(&mut self) -> Result<bool, String> {
        self.leaf.as_mut().unwrap().next()
    }

    fn get_data_rid(&self) -> Result<crate::record::rid::RID, String> {
        self.leaf.as_ref().unwrap().get_data_rid()
    }

    fn insert(
        &mut self,
        dataval: &Constant,
        datarid: crate::record::rid::RID,
    ) -> Result<(), String> {
        self.before_first(dataval)?;
        let e = self.leaf.as_mut().unwrap().insert(datarid)?;
        self.leaf.as_mut().unwrap().close()?;
        if let Some(entry) = e {
            let mut root = BTreeDir::new(
                self.tx.clone(),
                self.rootblk.clone(),
                self.dir_layout.clone(),
            )?;
            let e2 = root.insert(entry)?;
            if let Some(entry2) = e2 {
                root.make_new_root(entry2)?;
            }
            root.close()?;
        }

        Ok(())
    }

    fn delete(
        &mut self,
        dataval: &Constant,
        datarid: crate::record::rid::RID,
    ) -> Result<(), String> {
        self.before_first(dataval)?;
        let leaf = self.leaf.as_mut().unwrap();
        leaf.delete(datarid)?;
        leaf.close()?;

        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        if let Some(leaf) = self.leaf.as_mut() {
            leaf.close()?;
        }
        Ok(())
    }
}

pub fn search_cost(numblocks: i32, rpb: i32) -> i32 {
    (1. + f32::ln_1p(numblocks as f32) / f32::ln_1p(rpb as f32)) as i32
}
