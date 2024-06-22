#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::ops::{Bound, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{self, map_bound, MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path: &Path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // search in mem
        if let Some(result) = self.state.read().memtable.get(key) {
            if result.is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(result));
            }
        }

        for immutable in self.state.read().imm_memtables.iter() {
            if let Some(result) = immutable.get(key) {
                if result.is_empty() {
                    return Ok(None);
                } else {
                    return Ok(Some(result));
                }
            }
        }

        // search in sst
        for sst in &self.state.read().l0_sstables {
            let itr = SsTableIterator::create_and_seek_to_key(
                self.state.read().sstables.get(sst).unwrap().clone(),
                KeySlice::from_slice(key),
            )?;
            if itr.is_valid() && itr.key().into_inner().eq(key) {
                if itr.value().is_empty() {
                    return Ok(None);
                } else {
                    return Ok(Some(Bytes::copy_from_slice(itr.value())));
                }
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let mut memtable_reaches_capacity_on_put = {
            // acquire read lock on state
            let rw_lock_read_guard = self.state.read();

            // pay attention to memtable
            let metable = rw_lock_read_guard.memtable.clone();
            metable.put(_key, _value)?;
            metable.approximate_size().gt(&self.options.target_sst_size)
        };

        if memtable_reaches_capacity_on_put {
            // only one thread can switch memtable and acquire state_lock
            let state_lock = self.state_lock.lock();

            // check again
            memtable_reaches_capacity_on_put = {
                // acquire read lock on state
                let rw_lock_read_guard = self.state.read();
                rw_lock_read_guard
                    .memtable
                    .clone()
                    .approximate_size()
                    .gt(&self.options.target_sst_size)
            };

            if memtable_reaches_capacity_on_put {
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.put(_key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable = MemTable::create(self.options.target_sst_size);
        {
            let mut state = self.state.write();
            let state_data_mut = state.deref_mut();
            let ref_to_mem = state_data_mut.as_ref().memtable.clone();
            Arc::make_mut(state_data_mut)
                .imm_memtables
                .insert(0, ref_to_mem);
            Arc::make_mut(state_data_mut).memtable = Arc::new(memtable);
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // state_lock is for double checking
        let _state_lock = self.state_lock.lock();

        // minimize critical section
        let memtable_to_flush;
        {
            let guard = self.state.read();
            memtable_to_flush = guard.imm_memtables.last().unwrap().clone();
        };

        // build a new sst
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut sst_builder)?;
        let sst = sst_builder.build(
            self.next_sst_id(),
            Some(self.block_cache.clone()),
            self.path.join(
                self.next_sst_id
                    .load(std::sync::atomic::Ordering::SeqCst)
                    .to_string()
                    + ".sst",
            ),
        )?;

        // change the state of lsm
        {
            let mut state = self.state.write();
            Arc::make_mut(&mut state)
                .l0_sstables
                .insert(0, sst.sst_id());
            Arc::make_mut(&mut state)
                .sstables
                .insert(sst.sst_id(), Arc::new(sst));
            Arc::make_mut(&mut state).imm_memtables.pop();
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    // todo io improve
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        // get read lock
        let guard = self.state.read();

        // merge_iterator for memtable
        let merge_iterator_memtable = {
            let mut vec: Vec<Box<MemTableIterator>> = vec![];

            let mut closure = |memtable: &MemTable, lower: Bound<&[u8]>, upper: Bound<&[u8]>| {
                let memtable_iterator = memtable.scan(lower, upper);
                if memtable_iterator.is_valid() {
                    vec.push(Box::new(memtable_iterator));
                }
            };

            closure(&guard.memtable, lower, upper);
            for immutable_memtable in guard.imm_memtables.iter() {
                closure(immutable_memtable, lower, upper);
            }

            MergeIterator::create(vec)
        };

        // merge_iterator for sstable
        let merge_iterator_sstable = {
            let mut sst_vec: Vec<Box<SsTableIterator>> = Vec::new();
            for sst_id in &guard.l0_sstables {
                let sst = guard.sstables.get(sst_id).unwrap().clone();

                // filter impossible sst
                if !range_overlap(
                    (lower, upper),
                    (
                        sst.first_key().as_key_slice().into_inner(),
                        sst.last_key().as_key_slice().into_inner(),
                    ),
                ) {
                    continue;
                }

                // find block in sst
                let mut itr = Box::new(SsTableIterator::create_and_seek_to_key(
                    sst,
                    KeySlice::from_slice(match lower {
                        Bound::Included(i) => i,
                        Bound::Excluded(e) => e,
                        Bound::Unbounded => &[],
                    }),
                )?);
                // logic of start bound
                if let Bound::Excluded(e) = lower {
                    if itr.is_valid() && itr.key().into_inner().eq(e) {
                        itr.next()?;
                    };
                }
                // validity checking
                if itr.is_valid() {
                    sst_vec.push(itr);
                }
            }

            MergeIterator::create(sst_vec)
        };

        // constructing a TwoMergeIterator includes mem + sst
        let lsm_iterator_inner =
            TwoMergeIterator::create(merge_iterator_memtable, merge_iterator_sstable)?;

        // wrapping
        let fused_iterator =
            FusedIterator::new(LsmIterator::new(lsm_iterator_inner, map_bound(upper))?);

        Ok(fused_iterator)
    }
}

fn range_overlap(scan_range: (Bound<&[u8]>, Bound<&[u8]>), sst_range: (&[u8], &[u8])) -> bool {
    // scan low bound checking
    match scan_range.0 {
        Bound::Included(i) => {
            if i.gt(sst_range.1) {
                return false;
            };
        }
        Bound::Excluded(e) => {
            if e.ge(sst_range.1) {
                return false;
            };
        }
        _ => (),
    };

    // scan upper bound checking
    match scan_range.1 {
        Bound::Included(i) => {
            if i.lt(sst_range.0) {
                return false;
            };
        }
        Bound::Excluded(e) => {
            if e.le(sst_range.0) {
                return false;
            };
        }
        _ => (),
    };

    true
}
