#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::os::linux::raw::stat;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use nom::character::complete::tab;
use nom::complete::take;
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::{self, MergeIterator};
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let mut result = vec![];

        let snapshot = (*self.state.read()).clone();
        let sst_ids = match task {
            CompactionTask::Leveled(_) => todo!(),
            CompactionTask::Tiered(_) => todo!(),
            CompactionTask::Simple(task) => {
                let mut ssd_ids = vec![];
                for id in &task.upper_level_sst_ids {
                    ssd_ids.push(*id);
                }
                for id in &task.lower_level_sst_ids {
                    ssd_ids.push(*id);
                }
                ssd_ids
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut ssd_ids = vec![];
                for id in l0_sstables {
                    ssd_ids.push(*id);
                }
                for id in l1_sstables {
                    ssd_ids.push(*id);
                }
                ssd_ids
            }
        };

        let mut sst_iterators = vec![];
        for sst_id in sst_ids {
            let sst_iterator = SsTableIterator::create_and_seek_to_first(
                snapshot.sstables.get(&sst_id).unwrap().clone(),
            )?;
            sst_iterators.push(Box::new(sst_iterator));
        }
        let mut merge_iterator = MergeIterator::create(sst_iterators);

        let mut sst_table_builder = SsTableBuilder::new(self.options.block_size);
        while merge_iterator.is_valid() {
            if merge_iterator.value().is_empty() {
                merge_iterator.next()?;
                continue;
            }

            sst_table_builder.add(merge_iterator.key(), merge_iterator.value());
            if sst_table_builder
                .estimated_size()
                .ge(&self.options.target_sst_size)
            {
                let id = self.next_sst_id();
                let sst = sst_table_builder.build(
                    id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(id),
                )?;
                result.push(Arc::new(sst));
                sst_table_builder = SsTableBuilder::new(self.options.block_size);
            }

            merge_iterator.next()?;
        }

        let id = self.next_sst_id();
        let sst =
            sst_table_builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
        if !sst.first_key().is_empty() {
            result.push(Arc::new(sst));
        }

        Ok(result)
    }

    // TODO reconstructure by the functionality
    pub fn force_full_compaction(&self) -> Result<()> {
        // pin all sst
        let mut l0_sstables = vec![];
        let mut l1_sstables = vec![];
        {
            let state = self.state.read();
            for l0_sst in state.l0_sstables.iter() {
                l0_sstables.push(*l0_sst);
            }
            for l1_sst in state.levels[0].1.iter() {
                l1_sstables.push(*l1_sst);
            }
        };

        // full compaction
        let ssts_from_compact = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        })?;

        // update status of lsm
        {
            let mut state = self.state.write();

            // remove old sst
            let state_mutable = Arc::make_mut(state.deref_mut());
            state_mutable
                .l0_sstables
                .retain(|l0_sst| !l0_sstables.contains(l0_sst));
            state_mutable.levels[0].1.clear();
            state_mutable
                .sstables
                .retain(|sst, _| !l0_sstables.contains(sst) && !l1_sstables.contains(sst));
            // add new sst
            for sst in ssts_from_compact.into_iter() {
                state_mutable.levels[0].1.push(sst.sst_id());
                state_mutable.sstables.insert(sst.sst_id(), sst);
            }
        };

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        // make a snapshot
        let snapshot: Arc<LsmStorageState> = (*self.state.read()).clone();

        // generate a compaction task
        let compation_task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);

        // compact if need
        if let Some(task) = compation_task {
            // real compaction
            let output_sstables = self.compact(&task)?;

            // apply result
            let mut output_sst_ids = vec![];
            for output_sstable in output_sstables.iter() {
                output_sst_ids.push(output_sstable.sst_id());
            }
            let (mut new_lsm_storage_state, to_be_deleted_sst_ids) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output_sst_ids);

            // commit result, there might have gone throught flush within a small period of time
            {
                let mut write_guard = self.state.write();

                // 'new' a lsm_storage_state
                new_lsm_storage_state.memtable = write_guard.memtable.clone();
                new_lsm_storage_state.imm_memtables = write_guard.imm_memtables.clone();

                // dispose l0_sstables
                let mut l0_sstables = write_guard.l0_sstables.clone();
                l0_sstables.retain(|sst_id| !to_be_deleted_sst_ids.contains(sst_id));
                new_lsm_storage_state.l0_sstables = l0_sstables;

                // dispose sstables
                let mut sstables = write_guard.sstables.clone();
                sstables.retain(|k, _| !to_be_deleted_sst_ids.contains(k));
                for output_sstable in output_sstables {
                    sstables.insert(output_sstable.sst_id(), output_sstable);
                }
                new_lsm_storage_state.sstables = sstables;

                // 'swap'
                *write_guard = Arc::new(new_lsm_storage_state);
            }
        };

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if !self.state.read().imm_memtables.is_empty() {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
