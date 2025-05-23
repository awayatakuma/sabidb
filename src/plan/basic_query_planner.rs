use std::sync::{Arc, Mutex};

use crate::{metadata::matadata_manager::MetadataManager, parse::parser::Parser};

use super::{
    product_plan::ProductPlan, project_plan::ProjectPlan, query_planner::QueryPlanner,
    select_plan::SelectPlan, table_plan::TablePlan,
};

#[derive(Clone)]
pub struct BasicQueryPlanner {
    mdm: Arc<Mutex<MetadataManager>>,
}

impl QueryPlanner for BasicQueryPlanner {
    fn create_plan(
        &mut self,
        data: crate::parse::query_data::QueryData,
        tx: std::sync::Arc<std::sync::Mutex<crate::tx::transaction::Transaction>>,
    ) -> Result<Arc<Mutex<dyn super::plan::Plan>>, String> {
        //Step 1: Create a plan for each mentioned table or view.
        let mut plans = vec![];
        for tbl in data.tables() {
            let viewdef = self
                .mdm
                .lock()
                .map_err(|_| "failed to get lock")?
                .get_view_def(tbl.clone(), tx.clone())?;
            if let Some(vd) = viewdef {
                let mut parser = Parser::new(vd.as_str());
                let viewdata = parser.query().map_err(|_| "failed to create plan")?;
                let plan = self.create_plan(viewdata, tx.clone())?;
                plans.push(plan);
            } else {
                plans.push(Arc::new(Mutex::new(TablePlan::new(
                    tx.clone(),
                    tbl,
                    self.mdm.clone(),
                )?)));
            }
        }
        //Step 2: Create the product of all table plans
        let mut p = plans.remove(0);
        for nextplan in plans {
            p = Arc::new(Mutex::new(ProductPlan::new(p, nextplan)?));
        }

        //Step 3: Add a selection plan for the predicate
        p = Arc::new(Mutex::new(SelectPlan::new(p, data.pred())));

        //Step 4: Project on the field names
        p = Arc::new(Mutex::new(ProjectPlan::new(p, data.fields())?));

        Ok(p)
    }
}

impl BasicQueryPlanner {
    pub fn new(mdm: Arc<Mutex<MetadataManager>>) -> Self {
        BasicQueryPlanner { mdm: mdm }
    }
}
