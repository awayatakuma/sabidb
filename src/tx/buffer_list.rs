use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::{buffer::Buffer, buffer_manager::BufferManager},
    file::block_id::BlockId,
};

#[derive(Debug, Clone)]
pub struct BufferList {
    buffers: HashMap<BlockId, Arc<Mutex<Buffer>>>,
    pins: Vec<BlockId>,
    bm: Arc<Mutex<BufferManager>>,
}

impl BufferList {
    pub fn new_from_buffer_manager(bm: Arc<Mutex<BufferManager>>) -> Self {
        Self {
            buffers: HashMap::new(),
            pins: Vec::new(),
            bm: bm,
        }
    }

    pub(crate) fn get_buffer(&mut self, blk: &BlockId) -> Option<&mut Arc<Mutex<Buffer>>> {
        self.buffers.get_mut(blk)
    }

    pub(crate) fn pin(&mut self, blk: &BlockId) -> Result<(), String> {
        let buff = self
            .bm
            .lock()
            .map_err(|_| "failed to get lock")?
            .pin(blk)
            .map_err(|e| e.to_string())?
            .ok_or("you try to access an invalid buffer")?;
        self.buffers.insert(blk.clone(), buff);
        self.pins.push(blk.clone());
        Ok(())
    }

    pub(crate) fn unpin(&mut self, blk: &BlockId) -> Result<(), String> {
        let buff = self
            .buffers
            .get(blk)
            .ok_or("you try to access an invalid buffer")?;
        self.bm
            .lock()
            .map_err(|_| "failed to get lock")?
            .unpin(buff.clone())?;
        if let Some(remove_index) = self.pins.iter().position(|x| *x == *blk) {
            self.pins.remove(remove_index);
        }
        if !self.pins.contains(blk) {
            self.buffers.remove(blk);
        }
        Ok(())
    }

    pub(crate) fn unpin_all(&mut self) -> Result<(), String> {
        for blk in &self.pins {
            let buff = self
                .buffers
                .get(blk)
                .ok_or("you access to an invalid buffer")?;
            self.bm
                .lock()
                .map_err(|_| "failed to get lock")?
                .unpin(buff.clone())?;
        }
        self.buffers.clear();
        self.pins.clear();
        Ok(())
    }
}
