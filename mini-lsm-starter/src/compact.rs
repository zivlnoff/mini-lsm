#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::VecDeque;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use nom::character::complete::tab;
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

        // make a iterator on all compact sssts
        let mut merge_iterator = if let CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        } = task
        {
            let mut ssts_to_compact = vec![];
            {
                let state = self.state.read();
                for l0_sst in l0_sstables {
                    ssts_to_compact.push(state.sstables.get(l0_sst).unwrap().clone());
                }
                for l1_sst in l1_sstables {
                    ssts_to_compact.push(state.sstables.get(l1_sst).unwrap().clone());
                }
            }

            let mut sst_iterators = vec![];
            for sst in ssts_to_compact {
                let sst_iterator = SsTableIterator::create_and_seek_to_first(sst)?;
                sst_iterators.push(Box::new(sst_iterator));
            }

            MergeIterator::create(sst_iterators)
        } else {
            panic!("task must be ForceFullCompaction until now");
        };

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
            // state.levels[0].1.retain(|l1_sst| !l1_sstables.contains(l1_sst));
            state_mutable.levels[0].1.clear(); // full compaction always delete all l1 ssts and only current thread change l1 ssts
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
        unimplemented!()
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
