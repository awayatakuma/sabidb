use std::sync::{Arc, Mutex};

use crate::{
    parse::{
        create_index_data::CreateIndexData, create_table_data::CreateTableData,
        create_view_data::CreateViewData, delete_data::DeleteData, insert_data::InsertData,
        modify_data::ModifyData,
    },
    tx::transaction::Transaction,
};

pub trait UpdatePlanner: Sync + Send {
    fn execute_insert(&self, data: InsertData, tx: Arc<Mutex<Transaction>>) -> Result<i32, String>;
    fn execute_delete(&self, data: DeleteData, tx: Arc<Mutex<Transaction>>) -> Result<i32, String>;
    fn execute_modify(&self, data: ModifyData, tx: Arc<Mutex<Transaction>>) -> Result<i32, String>;
    fn execute_create_table(
        &self,
        data: CreateTableData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32, String>;
    fn execute_create_view(
        &self,
        data: CreateViewData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32, String>;
    fn execute_create_index(
        &self,
        data: CreateIndexData,
        tx: Arc<Mutex<Transaction>>,
    ) -> Result<i32, String>;
}
