#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::io::Read;
use std::os::unix::raw::off_t;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Error, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;
use nom::Err;

use crate::block::Block;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        // u32: offest u16:key_len key u16:key_len key
        for meta in block_meta.iter() {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put(meta.first_key.clone().into_inner());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put(meta.last_key.clone().into_inner());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = vec![];
        let mut vec: Vec<u8> = vec![];
        let _ = buf.reader().read_to_end(&mut vec);
        let mut arr_to_get = vec.as_slice();
        let mut index = 0;
        while arr_to_get.has_remaining() {
            let offset = arr_to_get.get_u32() as usize;
            index += 4;
            let first_key_len = arr_to_get.get_u16() as usize;
            index += 2;
            let first_key =
                KeyBytes::from_bytes(Bytes::copy_from_slice(&arr_to_get[0..first_key_len]));
            arr_to_get.advance(first_key_len);
            index += first_key_len;
            let last_key_len = arr_to_get.get_u16() as usize;
            index += 2;
            let last_key =
                KeyBytes::from_bytes(Bytes::copy_from_slice(&arr_to_get[0..last_key_len]));
            arr_to_get.advance(last_key_len);
            index += last_key_len;
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            })
        }

        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let block_meta_offset = file.read(file.size() - 4, 4)?.as_slice().get_u32() as usize;
        let block_meta = BlockMeta::decode_block_meta(
            file.read(
                block_meta_offset as u64,
                file.size() - 4 - block_meta_offset as u64,
            )?
            .as_slice(),
        );
        let first_key = block_meta[0].first_key.clone();
        let last_key = block_meta[block_meta.len() - 1].last_key.clone();

        let ss_table = SsTable {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        };

        Ok(ss_table)
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset_begin = self.block_meta[block_idx].offset as u64;
        let offset_end = if block_idx == self.block_meta.len() - 1 {
            self.block_meta_offset as u64
        } else {
            self.block_meta[block_idx + 1].offset as u64
        };
        let buf = self.file.read(offset_begin, offset_end - offset_begin)?;
        let block = Arc::new(Block::decode(buf.as_slice()));

        if self.block_cache.is_some() {
            // cache block
            self.block_cache
                .as_ref()
                .unwrap()
                .insert((self.sst_id(), block_idx), block.clone());
        }

        Ok(block)
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        match self
            .block_cache
            .as_ref()
            .unwrap()
            .try_get_with((self.sst_id(), block_idx), || self.read_block(block_idx))
        {
            Ok(block) => Ok(block),
            Err(err) => Err(Error::msg("read_block_cached err")),
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let mut left = 0;
        let mut right = self.block_meta.len() - 1;

        while left < right {
            let first_key_left_block = &self.block_meta[left].first_key;
            if key
                .into_inner()
                .eq(first_key_left_block.as_key_slice().into_inner())
            {
                break;
            }

            let first_key_right_block = &self.block_meta[right].first_key;
            if key
                .into_inner()
                .eq(first_key_right_block.as_key_slice().into_inner())
            {
                left = right;
                break;
            }

            let mid = (left + right + 1) / 2;
            let first_key_mid_block = &self.block_meta[mid].first_key;
            if key
                .into_inner()
                .gt(first_key_mid_block.as_key_slice().into_inner())
            {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        left
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
