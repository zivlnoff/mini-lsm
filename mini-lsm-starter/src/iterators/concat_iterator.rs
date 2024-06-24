#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{borrow::Borrow, sync::Arc};

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let current = SsTableIterator::create_and_seek_to_first(sstables[0].clone())?;
        let next_sst_idx = 1;

        Ok(SstConcatIterator {
            current: Some(current),
            next_sst_idx,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut current: Option<SsTableIterator> = None;
        let mut next_sst_idx = 0;
        for (idx, sst) in sstables.iter().enumerate() {
            if key.into_inner().le(sst.last_key().raw_ref()) {
                current = Some(SsTableIterator::create_and_seek_to_key(sst.clone(), key)?);
                next_sst_idx = idx + 1;
                break;
            }
        }

        Ok(SstConcatIterator {
            current,
            next_sst_idx,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        if let Some(itr) = &self.current {
            itr.key()
        } else {
            KeySlice::from_slice(&[])
        }
    }

    fn value(&self) -> &[u8] {
        if let Some(itr) = &self.current {
            itr.value()
        } else {
            &[]
        }
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if let Some(itr) = &mut self.current {
            itr.next()?;
            if !itr.is_valid() {
                if self.next_sst_idx < self.sstables.len() {
                    let itr_next = SsTableIterator::create_and_seek_to_first(
                        self.sstables[self.next_sst_idx].clone(),
                    )?;
                    self.current.replace(itr_next);

                    self.next_sst_idx += 1;
                } else {
                    let _ = self.current.take();
                };
            }
        };

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
