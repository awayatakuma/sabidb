use std::sync::{Arc, Mutex};

use crate::metadata::matadata_manager::MetadataManager;

use super::{
    plan::Plan, select_plan::SelectPlan, table_plan::TablePlan, update_planner::UpdatePlanner,
};

#[derive(Clone)]
pub struct BasicUpdatePlanner {
    mdm: Arc<Mutex<MetadataManager>>,
}

impl UpdatePlanner for BasicUpdatePlanner {
    fn execute_insert(
        &self,
        data: crate::parse::insert_data::InsertData,
        tx: Arc<Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<i32, String> {
        let mut p = TablePlan::new(tx, data.table_name(), self.mdm.clone())?;

        let mut s = p.open()?;
        let us = s.to_update_scan()?;

        us.insert()?;
        for (fldname, val) in data.fields().iter().zip(data.vals().iter()) {
            us.set_val(fldname.clone(), val.clone())?;
        }
        us.close().unwrap();

        Ok(1)
    }

    fn execute_delete(
        &self,
        data: crate::parse::delete_data::DeleteData,
        tx: Arc<Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<i32, String> {
        let p: Box<dyn Plan> = Box::new(TablePlan::new(tx, data.table_name(), self.mdm.clone())?);
        let mut sp = SelectPlan::new(p, data.pred());
        let mut count = 0;

        let mut s = sp.open()?;

        while s.next()? {
            s.to_update_scan()?.delete()?;
            count += 1;
        }
        s.close().unwrap();

        Ok(count)
    }

    fn execute_modify(
        &self,
        data: crate::parse::modify_data::ModifyData,
        tx: Arc<Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<i32, String> {
        let p = Box::new(TablePlan::new(tx, data.table_name(), self.mdm.clone()).unwrap());
        let mut sp = SelectPlan::new(p, data.pred());
        let mut s = sp.open()?;

        let mut count = 0;
        while s.next()? {
            let val = data.new_val().evaluate(&s)?;
            s.to_update_scan()?.set_val(data.target_field(), val)?;
            count += 1;
        }
        s.close()?;

        Ok(count)
    }

    fn execute_create_table(
        &self,
        data: crate::parse::create_table_data::CreateTableData,
        tx: Arc<Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<i32, String> {
        self.mdm
            .lock()
            .map_err(|_| "failed to get lock")?
            .create_table(
                data.table_name(),
                Arc::new(Mutex::new(data.new_schema())),
                tx,
            )?;
        Ok(0)
    }

    fn execute_create_view(
        &self,
        data: crate::parse::create_view_data::CreateViewData,
        tx: Arc<Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<i32, String> {
        self.mdm
            .lock()
            .map_err(|_| "failed to get lock")?
            .create_view(data.view_name(), data.view_def(), tx)?;
        Ok(0)
    }

    fn execute_create_index(
        &self,
        data: crate::parse::create_index_data::CreateIndexData,
        tx: Arc<Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<i32, String> {
        self.mdm
            .lock()
            .map_err(|_| "failed to get lock")?
            .create_index(data.idx_name(), data.table_name(), data.field_name(), tx)?;
        Ok(0)
    }
}

impl BasicUpdatePlanner {
    pub fn new(mdm: Arc<Mutex<MetadataManager>>) -> Self {
        BasicUpdatePlanner { mdm: mdm }
    }
}
