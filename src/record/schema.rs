use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub(crate) mod field_type {
    pub const INTEGER: i32 = 4;
    pub const VARCHAR: i32 = 12;
}

#[derive(Debug, Clone)]
struct FieldInfo {
    field_type: i32,
    length: i32,
}

impl FieldInfo {
    fn new(field_type: i32, length: i32) -> Self {
        Self {
            field_type: field_type,
            length: length,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    fields: Arc<Mutex<Vec<String>>>,
    info: Arc<Mutex<HashMap<String, FieldInfo>>>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            fields: Arc::new(Mutex::new(Vec::new())),
            info: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn add_field(
        &mut self,
        fldname: &String,
        field_type: i32,
        length: i32,
    ) -> Result<(), String> {
        self.fields
            .lock()
            .map_err(|_| "failed to get lock")?
            .push(fldname.clone());
        self.info
            .lock()
            .map_err(|_| "failed to get lock")?
            .insert(fldname.to_string(), FieldInfo::new(field_type, length));
        Ok(())
    }

    pub fn add_int_field(&mut self, fldname: &String) -> Result<(), String> {
        self.add_field(fldname, field_type::INTEGER, 0)
    }

    pub fn add_string_field(&mut self, fldname: &String, length: i32) -> Result<(), String> {
        self.add_field(fldname, field_type::VARCHAR, length)
    }

    pub fn add(&mut self, fldname: &String, sch: Arc<Mutex<Schema>>) -> Result<(), String> {
        let sch = sch.lock().map_err(|_| "failed to get lock")?;
        let field_type = sch.field_type(fldname)?;
        let length = sch.length(fldname)?;
        self.add_field(fldname, field_type, length)
    }

    pub fn add_all(&mut self, sch: Arc<Mutex<Schema>>) -> Result<(), String> {
        let fldnames = {
            sch.lock()
                .map_err(|_| "failed to get lock")?
                .fields
                .lock()
                .map_err(|_| "failed to get lock")?
                .clone()
        };
        for fldname in fldnames.iter() {
            self.add(fldname, sch.clone())?;
        }

        Ok(())
    }

    pub fn fields(&self) -> Arc<Mutex<Vec<String>>> {
        self.fields.clone()
    }

    pub fn has_field(&self, fldname: &String) -> Result<bool, String> {
        let ret = self
            .fields
            .lock()
            .map_err(|_| "failed to get lock")?
            .contains(fldname);
        Ok(ret)
    }

    pub fn field_type(&self, fldname: &String) -> Result<i32, String> {
        let ret = self
            .info
            .lock()
            .map_err(|_| "failed to get lock")?
            .get(fldname)
            .unwrap()
            .field_type;
        Ok(ret)
    }

    pub fn length(&self, fldname: &String) -> Result<i32, String> {
        let ret = self
            .info
            .lock()
            .map_err(|_| "failed to get lock")?
            .get(fldname)
            .unwrap()
            .length;
        Ok(ret)
    }
}
