#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::io::Read;
use std::sync::Arc;
use std::{panic::set_hook, path::Path};

use anyhow::{Ok, Result};
use bytes::{Buf, BufMut, Bytes};

use super::{bloom, BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![],
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.builder.add(key, value) {
            if self.first_key.is_empty() {
                self.first_key = key.into_inner().to_vec();
            }
        } else {
            let offset = self.data.len();
            let block =
                std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size)).build();

            let block_offsets = &block.offsets;
            let first_entry_offset = block_offsets[0] as usize;
            let first_key_len = (&block.data[first_entry_offset..]).get_u16() as usize;
            let first_key = KeyBytes::from_bytes(Bytes::copy_from_slice(
                &block.data[first_entry_offset + 2..first_entry_offset + 2 + first_key_len],
            ));
            let last_entry_offset = block_offsets[block_offsets.len() - 1] as usize;
            let last_ken_len = (&block.data[last_entry_offset..]).get_u16() as usize;
            let last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(
                &block.data[last_entry_offset + 2..last_entry_offset + 2 + last_ken_len],
            ));

            let block_data = BlockMeta {
                offset,
                first_key,
                last_key,
            };
            self.meta.push(block_data);

            let encoded_bytes = block.encode();
            self.data.put(encoded_bytes);

            let _ = self.builder.add(key, value);
        }

        self.last_key = key.into_inner().to_vec();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut block_meta = self.meta;
        let mut data = self.data;

        if !self.builder.is_empty() {
            let offset = data.len();
            let block = self.builder.build();

            let block_offsets = &block.offsets;
            let first_entry_offset = block_offsets[0] as usize;
            let first_key_len = (&block.data[first_entry_offset..]).get_u16() as usize;
            let first_key = KeyBytes::from_bytes(Bytes::copy_from_slice(
                &block.data[first_entry_offset + 2..first_entry_offset + 2 + first_key_len],
            ));
            let last_entry_offset = block_offsets[block_offsets.len() - 1] as usize;
            let last_ken_len = (&block.data[last_entry_offset..]).get_u16() as usize;
            let last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(
                &block.data[last_entry_offset + 2..last_entry_offset + 2 + last_ken_len],
            ));

            let block_data = BlockMeta {
                offset,
                first_key,
                last_key,
            };

            block_meta.push(block_data);

            let encoded_bytes = block.encode();
            data.put(encoded_bytes);
        }

        let meta_block_offset = data.len();
        BlockMeta::encode_block_meta(&block_meta, &mut data);
        data.put_u32(meta_block_offset as u32);

        // write to path
        let file_object = FileObject::create(path.as_ref(), data)?;

        Ok(SsTable {
            file: file_object,
            block_meta,
            block_meta_offset: meta_block_offset,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(self.first_key.as_slice())),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(self.last_key.as_slice())),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
