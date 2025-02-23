use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{constants::INTEGER_BYTES, file::page::Page};

use super::schema::{field_type, Schema};

#[derive(Clone, Debug)]
pub struct Layout {
    schema: Arc<Mutex<Schema>>,
    offsets: Arc<Mutex<HashMap<String, usize>>>,
    slotsize: i32,
}

impl Layout {
    pub fn new(
        schema: Arc<Mutex<Schema>>,
        offsets: Arc<Mutex<HashMap<String, usize>>>,
        slotsize: i32,
    ) -> Self {
        Layout {
            schema: schema,
            offsets: offsets,
            slotsize: slotsize,
        }
    }

    pub fn new_from_schema(schema: Arc<Mutex<Schema>>) -> Result<Self, String> {
        let mut offsets = HashMap::<String, usize>::new();
        let mut pos = INTEGER_BYTES;

        let binding = schema.lock().map_err(|_| "failed to get lock")?.fields();
        let binding = binding.lock().map_err(|_| "failed to get lock")?;
        let fldnames = binding.iter();

        for fldname in fldnames {
            offsets.insert(fldname.clone(), pos);
            pos += Self::length_in_bytes(fldname, schema.clone())?;
        }

        Ok(Layout {
            schema: schema,
            offsets: Arc::new(Mutex::new(offsets)),
            slotsize: pos as i32,
        })
    }

    pub fn schema(&self) -> Arc<Mutex<Schema>> {
        self.schema.clone()
    }

    pub fn offset(&self, fldname: &String) -> Result<usize, String> {
        let ret = self
            .offsets
            .lock()
            .map_err(|_| "failed to get lock")?
            .get(fldname)
            .unwrap()
            .clone();
        Ok(ret)
    }

    pub fn slot_size(&self) -> i32 {
        self.slotsize
    }

    fn length_in_bytes(fldname: &String, schema: Arc<Mutex<Schema>>) -> Result<usize, String> {
        let schema = schema.lock().map_err(|_| "failed to get lock")?;
        let field_type = schema.field_type(fldname)?;
        match field_type {
            field_type::INTEGER => Ok(INTEGER_BYTES),
            field_type::VARCHAR => Ok(Page::max_length(schema.length(fldname)? as usize)),
            _ => panic!("unreachable!!"),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::{Arc, Mutex};

    use crate::record::schema::Schema;

    use super::Layout;

    #[test]
    fn test_layout() {
        let mut sch = Schema::new();
        sch.add_int_field(&"A".to_string()).unwrap();
        sch.add_string_field(&"B".to_string(), 9).unwrap();
        let layout = Layout::new_from_schema(Arc::new(Mutex::new(sch))).unwrap();

        for fldname in layout
            .schema()
            .lock()
            .unwrap()
            .fields()
            .lock()
            .unwrap()
            .iter()
        {
            let offset = layout.offset(fldname).unwrap();
            if fldname == "A" {
                assert_eq!(offset, 4)
            } else if fldname == "B" {
                assert_eq!(offset, 8)
            } else {
                panic!("unreachable!!")
            }
        }
    }
}
