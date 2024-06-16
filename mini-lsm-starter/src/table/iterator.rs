#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{env::join_paths, sync::Arc};

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block(0)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(block);

        Ok(SsTableIterator {
            table,
            blk_iter,
            blk_idx: 0,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        self.blk_iter = BlockIterator::create_and_seek_to_first(self.table.read_block(0)?);

        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let block_meta = &table.block_meta;
        let mut left = 0;
        let mut right = block_meta.len() - 1;
        while left < right {
            let first_key_left_block = &block_meta[left].first_key;
            if key
                .into_inner()
                .eq(first_key_left_block.as_key_slice().into_inner())
            {
                break;
            }

            let first_key_right_block = &block_meta[right].first_key;
            if key
                .into_inner()
                .eq(first_key_right_block.as_key_slice().into_inner())
            {
                left = right;
                break;
            }

            let mid = (left + right + 1) / 2;
            let first_key_mid_block = &block_meta[mid].first_key;
            if key
                .into_inner()
                .gt(first_key_mid_block.as_key_slice().into_inner())
            {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        let mut blk_iter = BlockIterator::create_and_seek_to_key(table.read_block(left)?, key);
        if !blk_iter.is_valid() {
            left += 1;
            blk_iter = BlockIterator::create_and_seek_to_first(table.read_block(left + 1)?);
        }

        Ok(SsTableIterator {
            table,
            blk_iter,
            blk_idx: left,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let table = &self.table;
        let block_meta = &self.table.block_meta;
        let mut left = 0;
        let mut right = block_meta.len() - 1;
        while left < right {
            let first_key_left_block = &block_meta[left].first_key;
            if key
                .into_inner()
                .eq(first_key_left_block.as_key_slice().into_inner())
            {
                break;
            }

            let first_key_right_block = &block_meta[right].first_key;
            if key
                .into_inner()
                .eq(first_key_right_block.as_key_slice().into_inner())
            {
                left = right;
                break;
            }

            let mid = (left + right + 1) / 2;

            let first_key_mid_block = &block_meta[mid].first_key;
            if key
                .into_inner()
                .eq(first_key_mid_block.as_key_slice().into_inner())
            {
                left = mid;
                break;
            }

            if key
                .into_inner()
                .gt(first_key_mid_block.as_key_slice().into_inner())
            {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        if left == block_meta.len() {
            left -= 1;
        }

        let mut blk_iter = BlockIterator::create_and_seek_to_key(table.read_block(left)?, key);
        if !blk_iter.is_valid() && left < block_meta.len() - 1 {
            left += 1;
            blk_iter = BlockIterator::create_and_seek_to_first(table.read_block(left)?);
        }

        self.blk_idx = left;
        self.blk_iter = blk_iter;

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.block_meta.len() {
                self.blk_iter =
                    BlockIterator::create_and_seek_to_first(self.table.read_block(self.blk_idx)?);
            }
        }

        Ok(())
    }
}
