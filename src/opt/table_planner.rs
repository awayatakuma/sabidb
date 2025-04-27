use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    index::planner::{index_join_plan::IndexJoinPlan, index_select_plan::IndexSelectPlan},
    metadata::{index_info::IndexInfo, matadata_manager::MetadataManager},
    multibuffer::multibuffer_product_plan::MultibufferProductPlan,
    plan::{plan::Plan, select_plan::SelectPlan, table_plan::TablePlan},
    query::predicate::Predicate,
    record::schema::Schema,
    tx::transaction::Transaction,
};

pub struct TablePlanner {
    myplan: Arc<Mutex<TablePlan>>,
    mypred: Predicate,
    myschema: Arc<Mutex<Schema>>,
    indexes: HashMap<String, IndexInfo>,
    tx: Arc<Mutex<Transaction>>,
}

impl TablePlanner {
    pub fn new(
        tblname: String,
        mypred: Predicate,
        tx: Arc<Mutex<Transaction>>,
        mdm: Arc<Mutex<MetadataManager>>,
    ) -> Result<Self, String> {
        let myplan = TablePlan::new(tx.clone(), tblname.clone(), mdm.clone())?;
        let myschema = Arc::new(Mutex::new(myplan.schema()?));
        Ok(TablePlanner {
            myplan: Arc::new(Mutex::new(myplan)),
            mypred,
            myschema: myschema,
            indexes: mdm
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_index_info(tblname, tx.clone())?,
            tx: tx,
        })
    }

    pub fn make_select_plan(&self) -> Result<Arc<Mutex<dyn Plan>>, String> {
        if let Some(p) = self.make_index_select()? {
            self.add_select_pred(p)
        } else {
            self.add_select_pred(self.myplan.clone())
        }
    }

    pub fn make_join_plan(
        &self,
        current: Arc<Mutex<dyn Plan>>,
    ) -> Result<Option<Arc<Mutex<dyn Plan>>>, String> {
        let currsch = Arc::new(Mutex::new(
            current.lock().map_err(|_| "failed to get lock")?.schema()?,
        ));
        if self
            .mypred
            .join_sub_pred(self.myschema.clone(), currsch.clone())?
            .is_none()
        {
            return Ok(None);
        } else {
            let p = self.make_index_join(current.clone(), currsch.clone())?;
            if p.is_some() {
                return Ok(p);
            } else {
                return Ok(Some(self.make_product_join(current, currsch)?));
            }
        }
    }

    pub fn make_product_plan(
        &self,
        current: Arc<Mutex<dyn Plan>>,
    ) -> Result<Arc<Mutex<dyn Plan>>, String> {
        let p = self.add_select_pred(self.myplan.clone())?;
        Ok(Arc::new(Mutex::new(MultibufferProductPlan::new(
            self.tx.clone(),
            current,
            p,
        )?)))
    }

    fn make_index_select(&self) -> Result<Option<Arc<Mutex<dyn Plan>>>, String> {
        for fldname in self.indexes.keys() {
            if let Some(val) = self.mypred.equate_with_constant(fldname) {
                let ii = self.indexes.get(fldname).unwrap();
                println!("index on {} used", fldname);
                return Ok(Some(Arc::new(Mutex::new(IndexSelectPlan::new(
                    self.myplan.clone(),
                    ii.clone(),
                    val,
                )))));
            }
        }
        Ok(None)
    }

    fn make_index_join(
        &self,
        current: Arc<Mutex<dyn Plan>>,
        currsch: Arc<Mutex<Schema>>,
    ) -> Result<Option<Arc<Mutex<dyn Plan>>>, String> {
        for fldname in self.indexes.keys() {
            let outerfield = self.mypred.equate_with_field(fldname);
            if let Some(outerfield) = outerfield {
                if currsch
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .has_field(fldname)?
                {
                    continue;
                }
                let ii = self.indexes.get(&outerfield).unwrap();
                let p = Arc::new(Mutex::new(IndexJoinPlan::new(
                    current.clone(),
                    self.myplan.clone(),
                    ii.clone(),
                    outerfield,
                )?));
                let sp = self.add_select_pred(p)?;
                return Ok(Some(self.add_join_pred(sp, currsch.clone())?));
            }
        }

        Ok(None)
    }

    fn make_product_join(
        &self,
        current: Arc<Mutex<dyn Plan>>,
        currsch: Arc<Mutex<Schema>>,
    ) -> Result<Arc<Mutex<dyn Plan>>, String> {
        let p = self.make_product_plan(current)?;
        self.add_join_pred(p, currsch)
    }

    fn add_select_pred(&self, p: Arc<Mutex<dyn Plan>>) -> Result<Arc<Mutex<dyn Plan>>, String> {
        let selectpred = self.mypred.select_sub_pred(self.myschema.clone())?;
        if let Some(selectpred) = selectpred {
            Ok(Arc::new(Mutex::new(SelectPlan::new(p, selectpred))))
        } else {
            Ok(p)
        }
    }

    fn add_join_pred(
        &self,
        p: Arc<Mutex<dyn Plan>>,
        currsch: Arc<Mutex<Schema>>,
    ) -> Result<Arc<Mutex<dyn Plan>>, String> {
        let joinpred = self.mypred.join_sub_pred(self.myschema.clone(), currsch)?;
        if let Some(joinpred) = joinpred {
            Ok(Arc::new(Mutex::new(SelectPlan::new(p, joinpred))))
        } else {
            Ok(p)
        }
    }
}
