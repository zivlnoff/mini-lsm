#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{borrow::Borrow, sync::Arc};

use bytes::{Buf, Bytes};
use clap::Id;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut block_iterator = Self::new(block);
        Self::seek_to_first(&mut block_iterator);
        block_iterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut block_iterator = Self::new(block);
        Self::seek_to_key(&mut block_iterator, key);
        block_iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let first_key_len = (&self.block.data[0..]).get_u16() as usize;
        let first_value_len = (&self.block.data[2 + first_key_len..]).get_u16() as usize;
        self.key = KeyVec::from_vec(Vec::from(&self.block.data[2..2 + first_key_len]));
        self.value_range = (
            2 + first_key_len + 2,
            2 + first_key_len + 2 + first_value_len,
        );
        self.first_key = self.key.clone();
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx == self.block.offsets.len() - 1 {
            // reach the end
            self.key = KeyVec::new();
            return;
        }

        let next_key_len = (&self.block.data[self.value_range.1..]).get_u16() as usize;
        let next_value_len =
            (&self.block.data[self.value_range.1 + 2 + next_key_len..]).get_u16() as usize;
        self.key = KeyVec::from_vec(Vec::from(
            &self.block.data[self.value_range.1 + 2..self.value_range.1 + 2 + next_key_len],
        ));
        self.value_range = (
            self.value_range.1 + 2 + next_key_len + 2,
            self.value_range.1 + 2 + next_key_len + 2 + next_value_len,
        );
        self.idx += 1;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // binary search
        let key_bytes = key.into_inner();
        let mut left = 0;
        let mut right = self.block.offsets.len() - 1;
        while left < right {
            let mid = (left + right) / 2;
            let key_len =
                ((&self.block.data[self.block.offsets[mid] as usize..]).get_u16()) as usize;
            let key = &self.block.data[self.block.offsets[mid] as usize + 2
                ..self.block.offsets[mid] as usize + 2 + key_len];
            if key_bytes.le(key) {
                right = mid;
            } else {
                left = mid + 1;
            }
        }

        // left==right is final offset
        let key_len = ((&self.block.data[self.block.offsets[left] as usize..]).get_u16()) as usize;
        let value_len = ((&self.block.data[self.block.offsets[left] as usize + 2 + key_len..])
            .get_u16()) as usize;
        self.key = KeyVec::from_vec(Vec::from(
            &self.block.data[self.block.offsets[left] as usize + 2
                ..self.block.offsets[left] as usize + 2 + key_len],
        ));
        self.value_range = (
            self.block.offsets[left] as usize + 2 + key_len + 2,
            self.block.offsets[left] as usize + 2 + key_len + 2 + value_len,
        );
        self.idx = left;
    }
}
