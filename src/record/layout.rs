use std::{
    collections::HashMap,
    sync::Arc,
};

use crate::{constants::INTEGER_BYTES, file::page::Page};

use super::schema::{field_type, Schema};

#[derive(Clone, Debug)]
pub struct Layout {
    schema: Schema,
    offsets: Arc<HashMap<String, usize>>,
    slotsize: i32,
}

impl Layout {
    pub fn new(
        schema: Schema,
        offsets: Arc<HashMap<String, usize>>,
        slotsize: i32,
    ) -> Self {
        Layout {
            schema,
            offsets,
            slotsize,
        }
    }

    pub fn new_from_schema(schema: Schema) -> Result<Self, String> {
        let mut offsets = HashMap::<String, usize>::new();
        let mut pos = INTEGER_BYTES as usize;

        let fldnames = schema.fields();
        let fldnames_guard = fldnames.lock().map_err(|_| "failed to get lock")?;
        for fldname in fldnames_guard.iter() {
            offsets.insert(fldname.clone(), pos);
            pos += Self::length_in_bytes(fldname, &schema)?;
        }

        Ok(Layout {
            schema: schema.clone(),
            offsets: Arc::new(offsets),
            slotsize: pos as i32,
        })
    }

    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }

    pub fn offset(&self, fldname: &String) -> Result<usize, String> {
        Ok(*self
            .offsets
            .get(fldname)
            .ok_or_else(|| format!("field {} not found in layout", fldname))?)
    }

    pub fn slot_size(&self) -> i32 {
        self.slotsize
    }

    fn length_in_bytes(fldname: &String, schema: &Schema) -> Result<usize, String> {
        let field_type = schema.field_type(fldname)?;
        match field_type {
            field_type::INTEGER => Ok(INTEGER_BYTES as usize),
            field_type::BOOLEAN => Ok(INTEGER_BYTES as usize),
            field_type::VARCHAR => Ok(Page::max_length(schema.length(fldname)? as usize)),
            _ => panic!("unreachable!!"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::record::schema::Schema;

    use super::Layout;

    #[test]
    fn test_layout() {
        let sch = Schema::new();
        sch.add_int_field(&"A".to_string()).unwrap();
        sch.add_string_field(&"B".to_string(), 9).unwrap();
        let layout = Layout::new_from_schema(sch).unwrap();

        let sch2 = layout.schema();
        let binding = sch2.fields();
        let fldnames = binding.lock().unwrap();
        for fldname in fldnames.iter() {
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
