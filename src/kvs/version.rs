use crate::kvs::error::Error;
use crate::kvs::txn::TxnId;
use std::sync::RwLock;

pub type VersionId = usize;

#[derive(Clone)]
pub enum Version {
    Value(Vec<u8>),
    Deleted,
}

enum VersionWriteLockState {
    Unlocked,
    Locked(TxnId),
}

enum VersionVisibility {
    // The version is visible only to this transaction.
    // Used for uncommitted changes.
    OnlyTxn { txn_id: TxnId },

    // The version is visible to any transaction during or after the timestamp.
    // Used for the newest committed version in the version chain.
    AnyTxnDuringOrAfter { begin_ts: TxnId },

    // The version is visible to any transaction within the specified
    // time interval (inclusive).
    // Used for committed versions that have been superseded by newer committed versions.
    AnyTxnWithinTimeInterval { begin_ts: TxnId, end_ts: TxnId },
}

struct VersionEntry {
    // Txn holding the write lock for this version.
    write_lock_state: VersionWriteLockState,

    // Visibility of this version to transactions
    visibility: VersionVisibility,

    // Last txn to read this version
    read_ts: TxnId,

    // Previous version, if any.
    previous: Option<VersionId>,

    // Version data
    version: Version,
}

impl VersionEntry {
    fn new_uncommitted(txn_id: TxnId, v: Version, previous: Option<VersionId>) -> VersionEntry {
        VersionEntry {
            // txn holds the write lock until the version is committed
            write_lock_state: VersionWriteLockState::Locked(txn_id),

            // version is initially visible only to this transaction
            // because it has not yet been committed.
            visibility: VersionVisibility::OnlyTxn { txn_id: txn_id },

            // current txn is the first to read this version
            read_ts: txn_id,

            // link this version to the previous version, if any
            previous: previous,

            // actual data for this version
            version: v,
        }
    }

    fn is_visible_for_txn(&self, other_txn_id: TxnId) -> bool {
        match self.visibility {
            VersionVisibility::OnlyTxn { txn_id } => other_txn_id == txn_id,
            VersionVisibility::AnyTxnDuringOrAfter { begin_ts } => other_txn_id >= begin_ts,
            VersionVisibility::AnyTxnWithinTimeInterval { begin_ts, end_ts } => {
                other_txn_id >= begin_ts && other_txn_id <= end_ts
            }
        }
    }

    fn update_read_ts(&mut self, txn_id: TxnId) {
        if txn_id > self.read_ts {
            self.read_ts = txn_id;
        }
    }

    fn acquire_write_lock(&mut self, txn_id: TxnId) -> Result<bool, Error> {
        if self.read_ts > txn_id {
            // cannot update a version that has already been read by a later transaction.
            return Err(Error::ReadWriteConflict);
        }

        match self.write_lock_state {
            VersionWriteLockState::Unlocked => {
                self.write_lock_state = VersionWriteLockState::Locked(txn_id);
                Ok(true)
            }
            VersionWriteLockState::Locked(lock_txn_id) => {
                if lock_txn_id == txn_id {
                    // already had the write lock
                    Ok(false)
                } else {
                    // cannot update a version that is being written by another transaction
                    Err(Error::WriteWriteConflict)
                }
            }
        }
    }

    fn release_write_lock(&mut self) -> TxnId {
        match self.write_lock_state {
            VersionWriteLockState::Locked(txn_id) => {
                self.write_lock_state = VersionWriteLockState::Unlocked;
                txn_id
            }
            _ => panic!("Version write lock state is already unlocked"),
        }
    }

    fn set_visibility_after_commit(&mut self) {
        if let VersionVisibility::OnlyTxn { txn_id } = self.visibility {
            self.visibility = VersionVisibility::AnyTxnDuringOrAfter { begin_ts: txn_id };
        } else {
            panic!("Version visibility must be OnlyTxn");
        }
    }

    fn set_visibility_prev_after_commit(&mut self, end_ts: TxnId) {
        if let VersionVisibility::AnyTxnDuringOrAfter { begin_ts } = self.visibility {
            self.visibility = VersionVisibility::AnyTxnWithinTimeInterval { begin_ts, end_ts };
        } else {
            panic!("Version visibility must be AnyTxnDuringOrAfter");
        }
    }
}

pub struct VersionTable {
    entries: RwLock<Vec<RwLock<VersionEntry>>>,
}

impl VersionTable {
    pub fn new() -> VersionTable {
        VersionTable {
            entries: RwLock::new(Vec::new()),
        }
    }

    pub fn append_first_version(&self, txn_id: TxnId, v: Version) -> VersionId {
        let entry = VersionEntry::new_uncommitted(txn_id, v, None);
        let mut entries = self
            .entries
            .write()
            .expect("Could not acquire write lock on entries");
        entries.push(RwLock::new(entry));
        entries.len() - 1
    }

    pub fn append_next_version(
        &self,
        txn_id: TxnId,
        prev_version_id: VersionId,
        v: Version,
    ) -> Result<VersionId, Error> {
        let acquired = self.acquire_write_lock(txn_id, prev_version_id)?;
        if acquired {
            // acquired the write lock on the previous version,
            // so create a new version for the uncommitted changes
            let entry = VersionEntry::new_uncommitted(txn_id, v, Some(prev_version_id));
            let mut entries = self
                .entries
                .write()
                .expect("Could not acquire write lock on entries");
            entries.push(RwLock::new(entry));
            Ok(entries.len() - 1)
        } else {
            // already had a write lock on the existing version with uncommitted changes,
            // so update it in-place rather than creating a new version
            let entries = self
                .entries
                .read()
                .expect("Could not acquire read lock on entries");
            let mut entry = entries
                .get(prev_version_id)
                .ok_or(Error::VersionNotFound)?
                .write()
                .expect("Could not acquire write lock on entry");
            entry.version = v;
            Ok(prev_version_id)
        }
    }

    pub fn retrieve(&self, txn_id: TxnId, id: VersionId) -> Option<Version> {
        let mut current_id = id;
        loop {
            let entries = self
                .entries
                .read()
                .expect("Could not acquire read lock on entries");
            match entries.get(current_id) {
                None => {
                    return None;
                }
                Some(entry_lock) => {
                    let mut entry = entry_lock
                        .write()
                        .expect("Could not acquire write lock on entry");

                    if entry.is_visible_for_txn(txn_id) {
                        // found a version visible to this txn, so return it
                        entry.update_read_ts(txn_id);
                        return Some(entry.version.clone());
                    }

                    match entry.previous {
                        None => {
                            // no version is visible to this txn
                            return None;
                        }
                        Some(previous_id) => {
                            // follow the previous version
                            current_id = previous_id;
                        }
                    }
                }
            };
        }
    }

    pub fn commit(&self, version_id: VersionId) {
        let entries = self
            .entries
            .read()
            .expect("Could not acquire read lock on entries");

        let mut entry = entries
            .get(version_id)
            .expect("Could not find version")
            .write()
            .expect("Could not acquire write lock on entry");

        entry.set_visibility_after_commit();
        let txn_id = entry.release_write_lock();

        if let Some(prev_id) = entry.previous {
            let mut prev = entries
                .get(prev_id)
                .expect("Could not find previous version")
                .write()
                .expect("Could not acquire write lock on previous entry");
            prev.set_visibility_prev_after_commit(txn_id);
            prev.release_write_lock();
        }
    }

    pub fn abort(&self, version_id: VersionId) -> Option<VersionId> {
        let entries = self
            .entries
            .read()
            .expect("Could not acquire read lock on entries");

        let entry = entries
            .get(version_id)
            .expect("Could not find version")
            .read()
            .expect("Could not acquire write lock on entry");

        entry.previous.and_then(|prev_id| {
            let mut prev = entries
                .get(prev_id)
                .expect("Could not find previous version")
                .write()
                .expect("Could not acquire write lock on prev entry");
            prev.release_write_lock();
            Some(prev_id)
        })
    }

    fn acquire_write_lock(&self, txn_id: TxnId, version_id: VersionId) -> Result<bool, Error> {
        let entries = self
            .entries
            .read()
            .expect("Could not acquire read lock on entries");

        let mut entry = entries
            .get(version_id)
            .ok_or(Error::VersionNotFound)?
            .write()
            .expect("Could not acquire write lock on entry");

        entry.acquire_write_lock(txn_id)
    }
}
