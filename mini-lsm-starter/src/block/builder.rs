#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::{BufMut, BytesMut};

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::<u16>::new(),
            data: Vec::<u8>::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let encoded_block_size_before = self.offsets.len() * 2 + self.data.len() + 2;
        let encoded_block_size_after = encoded_block_size_before + key.len() + value.len() + 4 + 2;
        if encoded_block_size_after.gt(&self.block_size) && !self.offsets.is_empty() {
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        let mut bytes_mut = BytesMut::with_capacity(key.len() + value.len() + 4);
        bytes_mut.put_u16(key.len() as u16);
        bytes_mut.put(key.into_inner());
        bytes_mut.put_u16(value.len() as u16);
        bytes_mut.put(value);
        self.data.append(&mut Vec::from(bytes_mut));

        if self.first_key.is_empty() {
            self.first_key = KeyVec::from_vec(Vec::from(key.into_inner()))
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
