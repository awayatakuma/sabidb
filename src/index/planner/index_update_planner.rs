use std::sync::{Arc, Mutex};

use crate::{
    metadata::matadata_manager::MetadataManager,
    plan::{
        plan::Plan, select_plan::SelectPlan, table_plan::TablePlan, update_planner::UpdatePlanner,
    },
};

pub struct IndexUpdatePlanner {
    mdm: Arc<Mutex<MetadataManager>>,
}

impl IndexUpdatePlanner {
    pub fn new(mdm: Arc<Mutex<MetadataManager>>) -> Self {
        IndexUpdatePlanner { mdm: mdm }
    }
}

impl UpdatePlanner for IndexUpdatePlanner {
    fn execute_insert(
        &self,
        data: crate::parse::insert_data::InsertData,
        tx: std::sync::Arc<std::sync::Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<i32, String> {
        let tblname = data.table_name();
        let p = TablePlan::new(tx.clone(), tblname.clone(), self.mdm.clone())?;

        let s = p.open()?;
        let binding = s
            .lock()
            .map_err(|_| "failed to get lock")?
            .to_update_scan()?;
        let mut us = binding.lock().map_err(|_| "failed to get lock")?;

        us.insert()?;
        let rid = us.get_rid()?;

        let indexes = self
            .mdm
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_index_info(tblname, tx.clone())?;
        for (fldname, val) in data.fields().iter().zip(data.vals().iter()) {
            us.set_val(fldname.clone(), val.clone())?;
            if let Some(ii) = indexes.get(fldname) {
                let idx = ii.open()?;
                idx.lock()
                    .map_err(|_| "failed to get lock")?
                    .insert(val, rid.clone())?;
                idx.lock().map_err(|_| "failed to get lock")?.close()?;
            }
        }

        us.close()?;
        Ok(1)
    }

    fn execute_delete(
        &self,
        data: crate::parse::delete_data::DeleteData,
        tx: std::sync::Arc<std::sync::Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<i32, String> {
        let tblname = data.table_name();
        let p = TablePlan::new(tx.clone(), tblname.clone(), self.mdm.clone())?;
        let sp = SelectPlan::new(Arc::new(Mutex::new(p)), data.pred());

        let indexes = self
            .mdm
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_index_info(tblname.clone(), tx.clone())?;

        let s = sp.open()?;
        let binding = s
            .lock()
            .map_err(|_| "failed to get lock")?
            .to_update_scan()?;
        let mut us = binding.lock().map_err(|_| "failed to get lock")?;
        let mut count = 0;

        while us.next()? {
            let rid = us.get_rid()?;
            for fldname in indexes.keys() {
                let val = us.get_val(fldname)?;
                if let Some(idxinfo) = indexes.get(fldname) {
                    let idx = idxinfo.open()?;
                    idx.lock()
                        .map_err(|_| "failed to get lock")?
                        .delete(&val, rid.clone())?;
                    idx.lock().map_err(|_| "failed to get lock")?.close()?;
                }
            }
            us.delete()?;
            count += 1;
        }

        us.close()?;
        Ok(count)
    }

    fn execute_modify(
        &self,
        data: crate::parse::modify_data::ModifyData,
        tx: std::sync::Arc<std::sync::Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<i32, String> {
        let tblname = data.table_name();
        let fldname = data.target_field();
        let p = TablePlan::new(tx.clone(), tblname.clone(), self.mdm.clone())?;
        let p = SelectPlan::new(Arc::new(Mutex::new(p)), data.pred());

        let mp = self
            .mdm
            .lock()
            .map_err(|_| "failed to get lock")?
            .get_index_info(tblname.clone(), tx.clone())?;
        let ii = mp.get(&fldname);

        let mut idx = if let Some(idx) = ii {
            Some(idx.open()?)
        } else {
            None
        };

        let s = p.open()?;
        let binding = s
            .lock()
            .map_err(|_| "failed to get lock")?
            .to_update_scan()?;
        let mut us = binding.lock().map_err(|_| "failed to get lock")?;

        let mut count = 0;
        while us.next()? {
            let newval = data.new_val().evaluate(s.clone())?;
            let oldval = us.get_val(&fldname)?;
            us.set_val(fldname.clone(), newval.clone())?;

            if let Some(ref mut idx) = idx {
                let rid = us.get_rid()?;
                idx.lock()
                    .map_err(|_| "failed to get lock")?
                    .delete(&oldval, rid.clone())?;
                idx.lock()
                    .map_err(|_| "failed to get lock")?
                    .insert(&newval, rid)?;
            }
            count += 1;
        }

        if let Some(idx) = idx {
            idx.lock().map_err(|_| "failed to get lock")?.close()?;
        }

        us.close()?;

        Ok(count)
    }

    fn execute_create_table(
        &self,
        data: crate::parse::create_table_data::CreateTableData,
        tx: std::sync::Arc<std::sync::Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<i32, String> {
        self.mdm
            .lock()
            .map_err(|_| "failed to get lock")?
            .create_table(
                data.table_name(),
                Arc::new(Mutex::new(data.new_schema())),
                tx.clone(),
            )?;
        Ok(0)
    }

    fn execute_create_view(
        &self,
        data: crate::parse::create_view_data::CreateViewData,
        tx: std::sync::Arc<std::sync::Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<i32, String> {
        self.mdm
            .lock()
            .map_err(|_| "failed to get lock")?
            .create_view(data.view_name(), data.view_def(), tx.clone())?;
        Ok(0)
    }

    fn execute_create_index(
        &self,
        data: crate::parse::create_index_data::CreateIndexData,
        tx: std::sync::Arc<std::sync::Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<i32, String> {
        self.mdm
            .lock()
            .map_err(|_| "failed to get lock")?
            .create_index(
                data.idx_name(),
                data.table_name(),
                data.field_name(),
                tx.clone(),
            )?;
        Ok(0)
    }
}
