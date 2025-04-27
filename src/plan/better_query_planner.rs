use std::sync::{Arc, Mutex};

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
    ) -> Result<std::sync::Arc<std::sync::Mutex<(dyn Plan + 'static)>>, String> {
        //Step 1: Create a plan for each mentioned table or view.
        let mut plans = vec![];
        for tblname in data.tables() {
            if let Some(viewdef) = self.mdm.get_view_def(tblname.clone(), tx.clone())? {
                let mut parser = Parser::new(&viewdef);
                let viewdata = parser.query().map_err(|_| "failed to parse sql")?;
                let result = self.create_plan(viewdata, tx.clone())?;
                plans.push(result);
            } else {
                let p = Arc::new(Mutex::new(TablePlan::new(
                    tx.clone(),
                    tblname,
                    Arc::new(Mutex::new(self.mdm.clone())),
                )?));
                plans.push(p);
            }
        }

        //Step 2: Create the product of all table plans
        let mut p = plans.remove(0);
        for nextplan in plans {
            let choice1 = Arc::new(Mutex::new(ProductPlan::new(nextplan.clone(), p.clone())?));
            let choice2 = Arc::new(Mutex::new(ProductPlan::new(p, nextplan)?));
            if choice1
                .lock()
                .map_err(|_| "failed to get lock")?
                .blocks_accessed()
                < choice2
                    .lock()
                    .map_err(|_| "failed to get lock")?
                    .blocks_accessed()
            {
                p = choice1;
            } else {
                p = choice2;
            }
        }

        //Step 3: Add a selection plan for the predicate
        let sp = Arc::new(Mutex::new(SelectPlan::new(p, data.pred())));

        //Step 4: Project on the field names
        let ret = Arc::new(Mutex::new(ProjectPlan::new(sp, data.fields())?));

        Ok(ret)
    }
}

impl BetterQueryPlanner {
    pub fn _new(mdm: MetadataManager) -> Self {
        BetterQueryPlanner { mdm: mdm }
    }
}
