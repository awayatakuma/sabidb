use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
};

use crate::{metadata::matadata_manager::MetadataManager, parse::parser::Parser};

use super::{
    plan::Plan, product_plan::ProductPlan, project_plan::ProjectPlan, query_planner::QueryPlanner,
    select_plan::SelectPlan, table_plan::TablePlan,
};

#[derive(Clone)]
pub struct BetterQueryPlanner {
    mdm: MetadataManager,
}

impl QueryPlanner for BetterQueryPlanner {
    fn create_plan(
        &mut self,
        data: crate::parse::query_data::QueryData,
        tx: std::sync::Arc<std::sync::Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<Box<dyn super::plan::Plan>, String> {
        //Step 1: Create a plan for each mentioned table or view.
        let mut plans: Vec<Rc<RefCell<Box<dyn Plan>>>> = vec![];
        for tblname in data.tables() {
            if let Some(viewdef) = self.mdm.get_view_def(tblname.clone(), tx.clone())? {
                let mut parser = Parser::new(&viewdef);
                let viewdata = parser.query().map_err(|_| "failed to parse sql")?;
                let result = Rc::new(RefCell::new(self.create_plan(viewdata, tx.clone())?));
                plans.push(result);
            } else {
                let p: Box<dyn Plan> = Box::new(TablePlan::new(
                    tx.clone(),
                    tblname,
                    Arc::new(Mutex::new(self.mdm.clone())),
                )?);
                plans.push(Rc::new(RefCell::new(p)));
            }
        }

        //Step 2: Create the product of all table plans
        let mut p = plans.remove(0);
        for nextplan in plans {
            let choice1 = Box::new(ProductPlan::new(nextplan.clone(), p.clone())?);
            let choice2 = Box::new(ProductPlan::new(p, nextplan)?);
            if choice1.blocks_accessed() < choice2.blocks_accessed() {
                p = Rc::new(RefCell::new(choice1));
            } else {
                p = Rc::new(RefCell::new(choice2));
            }
        }

        //Step 3: Add a selection plan for the predicate
        let mut ret: Box<dyn Plan> = Box::new(SelectPlan::new(
            Rc::try_unwrap(p)
                .unwrap_or_else(|_| panic!("Failed to unwrap Rc"))
                .into_inner(),
            data.pred(),
        ));

        //Step 4: Project on the field names
        ret = Box::new(ProjectPlan::new(ret, data.fields()));

        Ok(ret)
    }
}

impl BetterQueryPlanner {
    pub fn new(mdm: MetadataManager) -> Self {
        BetterQueryPlanner { mdm: mdm }
    }
}
