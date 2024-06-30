use serde::{Deserialize, Serialize};

use crate::lsm_storage::{self, LsmStorageState};

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // level-0 trigger, the same as reference resolution, where i think has difference with doc
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let upper_level_sst_ids = snapshot.l0_sstables.clone();
            let lower_level_sst_ids = snapshot.levels[0].1.clone();
            if !upper_level_sst_ids.is_empty()
                && (lower_level_sst_ids.len() as f64 / upper_level_sst_ids.len() as f64)
                    < self.options.size_ratio_percent as f64 / 100.0
            {
                let simple_leveled_compaction_task = SimpleLeveledCompactionTask {
                    upper_level: None,
                    upper_level_sst_ids,
                    lower_level: 0,
                    lower_level_sst_ids,
                    is_lower_level_bottom_level: false,
                };
                return Some(simple_leveled_compaction_task);
            };
        };

        // size ratio trigger
        for lower_level in 1..snapshot.levels.len() {
            let upper_level_size = snapshot.levels[lower_level - 1].1.len();
            let lower_level_size = snapshot.levels[lower_level].1.len();
            if upper_level_size != 0
                && (lower_level_size as f64 / upper_level_size as f64)
                    < self.options.size_ratio_percent as f64 / 100.0
            {
                let upper_level_sst_ids = snapshot.levels[lower_level - 1].1.clone();
                let lower_level_sst_ids = snapshot.levels[lower_level].1.clone();

                let simple_leveled_compaction_task = SimpleLeveledCompactionTask {
                    upper_level: if lower_level == 0 {
                        None
                    } else {
                        Some(lower_level - 1)
                    },
                    upper_level_sst_ids,
                    lower_level,
                    lower_level_sst_ids,
                    is_lower_level_bottom_level: false,
                };
                return Some(simple_leveled_compaction_task);
            }
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_lsm_storage_state = snapshot.clone();
        match task.upper_level {
            // only one thread running compaction job
            None => {
                new_lsm_storage_state.l0_sstables = vec![];
                new_lsm_storage_state.levels[0].1 = output.to_vec();
            }
            Some(upper_level) => {
                new_lsm_storage_state.levels[upper_level].1 = vec![];
                new_lsm_storage_state.levels[upper_level + 1].1 = output.to_vec();
            }
        }

        let mut to_be_deleted = vec![];
        for upper_level_sst_id in &task.upper_level_sst_ids {
            to_be_deleted.push(*upper_level_sst_id);
        }
        for lower_level_sst_id in &task.lower_level_sst_ids {
            to_be_deleted.push(*lower_level_sst_id);
        }

        (new_lsm_storage_state, to_be_deleted)
    }
}
