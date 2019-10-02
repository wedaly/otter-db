use crate::encode::{BytesReader, BytesWriter, Decode, Encode};
use crate::kvs::error::Error;
use crate::kvs::txn::TxnId;
use std::sync::RwLock;

pub type VersionId = usize;

pub enum Version<'a, V>
where
    V: Encode,
{
    Deleted,
    Value(&'a V),
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

#[derive(Clone, Copy)]
struct ValueByteRange {
    start: usize, // inclusive
    end: usize,   // exclusive
}

const EMPTY_VALUE_BYTE_RANGE: ValueByteRange = ValueByteRange { start: 0, end: 0 };

struct VersionEntry {
    // Txn holding the write lock for this version.
    write_lock_state: VersionWriteLockState,

    // Visibility of this version to transactions
    visibility: VersionVisibility,

    // Last txn to read this version
    read_ts: TxnId,

    // Previous version, if any.
    previous: Option<VersionId>,

    // Whether the value was deleted in this version
    is_deleted: bool,

    // Value byte range (valid only if not deleted)
    val_byte_range: ValueByteRange,
}

impl VersionEntry {
    fn new_uncommitted(
        txn_id: TxnId,
        previous: Option<VersionId>,
        is_deleted: bool,
        val_byte_range: ValueByteRange,
    ) -> VersionEntry {
        VersionEntry {
            // txn holds the write lock until the version is committed
            write_lock_state: VersionWriteLockState::Locked(txn_id),

            // version is initially visible only to this transaction
            // because it has not yet been committed.
            visibility: VersionVisibility::OnlyTxn { txn_id: txn_id },

            // current txn is the first to read this version
            read_ts: txn_id,

            // link this version to the previous version, if any
            previous,

            // whether the value is deleted in this version
            is_deleted,

            // start/end byte range for the value
            val_byte_range,
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
    values: RwLock<Vec<u8>>,
}

impl VersionTable {
    pub fn new() -> VersionTable {
        VersionTable {
            entries: RwLock::new(Vec::new()),
            values: RwLock::new(Vec::new()),
        }
    }

    pub fn append_first_version<V>(&self, txn_id: TxnId, version: Version<V>) -> VersionId
    where
        V: Encode,
    {
        let prev = None;
        let (is_deleted, val_byte_range) = match version {
            Version::Deleted => (true, EMPTY_VALUE_BYTE_RANGE),
            Version::Value(val) => (false, self.write_value_bytes(val)),
        };
        let entry = VersionEntry::new_uncommitted(txn_id, prev, is_deleted, val_byte_range);
        let mut entries = self
            .entries
            .write()
            .expect("Could not acquire write lock on entries");
        entries.push(RwLock::new(entry));
        entries.len() - 1
    }

    pub fn append_next_version<V>(
        &self,
        txn_id: TxnId,
        prev_version_id: VersionId,
        version: Version<V>,
    ) -> Result<VersionId, Error>
    where
        V: Encode,
    {
        let (is_deleted, val_byte_range) = match version {
            Version::Deleted => (true, EMPTY_VALUE_BYTE_RANGE),
            Version::Value(val) => (false, self.write_value_bytes(val)),
        };
        let acquired = self.acquire_write_lock(txn_id, prev_version_id)?;
        if acquired {
            // acquired the write lock on the previous version,
            // so create a new version for the uncommitted changes
            let entry = VersionEntry::new_uncommitted(
                txn_id,
                Some(prev_version_id),
                is_deleted,
                val_byte_range,
            );
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
            entry.is_deleted = is_deleted;
            entry.val_byte_range = val_byte_range;
            Ok(prev_version_id)
        }
    }

    pub fn retrieve<V>(&self, txn_id: TxnId, id: VersionId) -> Result<Option<V>, Error>
    where
        V: Decode,
    {
        let mut current_id = id;
        let val_byte_range: ValueByteRange;
        loop {
            let entries = self
                .entries
                .read()
                .expect("Could not acquire read lock on entries");
            match entries.get(current_id) {
                None => {
                    return Ok(None);
                }
                Some(entry_lock) => {
                    let mut entry = entry_lock
                        .write()
                        .expect("Could not acquire write lock on entry");

                    if entry.is_visible_for_txn(txn_id) {
                        // found a version visible to this txn
                        entry.update_read_ts(txn_id);
                        if entry.is_deleted {
                            return Ok(None);
                        } else {
                            val_byte_range = entry.val_byte_range;
                            break; // exit the loop to release the lock on entries
                        }
                    }

                    match entry.previous {
                        None => {
                            // no version is visible to this txn
                            return Ok(None);
                        }
                        Some(previous_id) => {
                            // follow the previous version
                            current_id = previous_id;
                        }
                    }
                }
            };
        }

        // Found a non-deleted version visible to this txn, so return its value
        let values = self
            .values
            .read()
            .expect("Could not acquire read lock on value bytes");
        let val_slice = &values[val_byte_range.start..val_byte_range.end];
        let val = V::decode(&mut BytesReader::new(val_slice))?;
        Ok(Some(val))
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

    fn write_value_bytes<V>(&self, val: &V) -> ValueByteRange
    where
        V: Encode,
    {
        let mut values = self
            .values
            .write()
            .expect("Could not acquire write lock on value bytes");
        let start = values.len();
        let mut w = BytesWriter::new(&mut *values);
        val.encode(&mut w);
        ValueByteRange {
            start: start,
            end: values.len(),
        }
    }
}
