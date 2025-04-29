use std::{
    i32,
    sync::{Arc, Mutex},
};

use crate::{
    rdbc::{result_set_metadata_adapter::ResultSetMetadataAdapter, sql_exception::SQLException},
    record::schema::{field_type::INTEGER, Schema},
};

pub struct EmbeddedMetadata {
    sch: Arc<Mutex<Schema>>,
}

impl EmbeddedMetadata {
    pub fn new(sch: Arc<Mutex<Schema>>) -> Self {
        EmbeddedMetadata { sch }
    }
}

impl ResultSetMetadataAdapter for EmbeddedMetadata {
    fn get_column_count(&self) -> Result<i32, crate::rdbc::sql_exception::SQLException> {
        Ok(self
            .sch
            .lock()
            .map_err(|_| SQLException {})?
            .fields()
            .lock()
            .map_err(|_| SQLException {})?
            .len() as i32)
    }

    fn get_column_name(
        &self,
        column: i32,
    ) -> Result<Option<String>, crate::rdbc::sql_exception::SQLException> {
        Ok(self
            .sch
            .lock()
            .map_err(|_| SQLException {})?
            .fields()
            .lock()
            .map_err(|_| SQLException {})?
            .get(column as usize - 1)
            .cloned())
    }

    fn get_column_type(
        &self,
        column: i32,
    ) -> Result<Option<i32>, crate::rdbc::sql_exception::SQLException> {
        if let Some(fldname) = self.get_column_name(column)? {
            return Ok(Some(
                self.sch
                    .lock()
                    .map_err(|_| SQLException {})?
                    .field_type(&fldname)
                    .map_err(|_| SQLException {})?,
            ));
        }

        Ok(None)
    }

    fn get_column_display_size(
        &self,
        column: i32,
    ) -> Result<i32, crate::rdbc::sql_exception::SQLException> {
        if let Ok(Some(fldname)) = self.get_column_name(column) {
            let fldtype = self
                .sch
                .lock()
                .map_err(|_| SQLException {})?
                .field_type(&fldname)
                .map_err(|_| SQLException {})?;
            let fldlen = if fldtype == INTEGER {
                6
            } else {
                self.sch
                    .lock()
                    .map_err(|_| SQLException {})?
                    .length(&fldname)
                    .map_err(|_| SQLException {})?
            };
            return Ok(i32::max(fldname.len() as i32, fldlen) + 1);
        }
        Ok(0)
    }
}
