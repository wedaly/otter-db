use crate::kvs::error::Error;
use crate::kvs::key_space::{KeySpace, KeySpaceId};
use crate::kvs::txn::{TxnId, TxnManager};
use std::collections::HashSet;
use std::sync::RwLock;

pub struct Store {
    txn_manager: TxnManager,
    key_spaces: RwLock<Vec<KeySpace>>,
}

impl Store {
    pub fn new() -> Store {
        Store {
            txn_manager: TxnManager::new(),
            key_spaces: RwLock::new(vec![KeySpace::new()]),
        }
    }

    pub fn define_keyspace(&self) -> KeySpaceId {
        let mut key_spaces = self
            .key_spaces
            .write()
            .expect("Could not acquire write lock for key spaces");
        key_spaces.push(KeySpace::new());
        key_spaces.len() - 1
    }

    pub fn begin_txn(&self) -> TxnId {
        self.txn_manager.begin_txn()
    }

    pub fn commit_txn(&self, txn_id: TxnId) -> Result<(), Error> {
        self.txn_manager.commit_txn(
            txn_id,
            |key_space_id, key_set| self.commit_keys(key_space_id, key_set),
            |key_space_id, key_set| self.abort_keys(key_space_id, key_set),
        )
    }

    pub fn abort_txn(&self, txn_id: TxnId) -> Result<(), Error> {
        self.txn_manager.abort_txn(txn_id, |key_space_id, key_set| {
            self.abort_keys(key_space_id, key_set)
        })
    }

    pub fn get(
        &self,
        txn_id: TxnId,
        key_space_id_opt: Option<KeySpaceId>,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        self.check_is_valid_txn(txn_id)?;
        let key_space_id = key_space_id_opt.unwrap_or(0);
        let result = self
            .key_spaces
            .read()
            .expect("Could not acquire read lock on key spaces")
            .get(key_space_id)
            .ok_or(Error::UndefinedKeySpace)
            .and_then(|ks| ks.get(txn_id, key));

        if result.is_ok() {
            self.txn_manager.record_read(txn_id, key_space_id, key);
        }

        result
    }

    pub fn set(
        &self,
        txn_id: TxnId,
        key_space_id_opt: Option<KeySpaceId>,
        key: &[u8],
        val: &[u8],
    ) -> Result<(), Error> {
        self.check_is_valid_txn(txn_id)?;
        let key_space_id = key_space_id_opt.unwrap_or(0);
        let result = self
            .key_spaces
            .read()
            .expect("Could not acquire read lock on key spaces")
            .get(key_space_id)
            .ok_or(Error::UndefinedKeySpace)
            .and_then(|ks| ks.set(txn_id, key, val));

        if result.is_ok() {
            self.txn_manager.record_write(txn_id, key_space_id, key);
        }

        result
    }

    pub fn delete(
        &self,
        txn_id: TxnId,
        key_space_id_opt: Option<KeySpaceId>,
        key: &[u8],
    ) -> Result<(), Error> {
        self.check_is_valid_txn(txn_id)?;
        let key_space_id = key_space_id_opt.unwrap_or(0);
        let result = self
            .key_spaces
            .read()
            .expect("Could not acquire read lock on key spaces")
            .get(key_space_id)
            .ok_or(Error::UndefinedKeySpace)
            .and_then(|ks| ks.delete(txn_id, key));

        if result.is_ok() {
            self.txn_manager.record_write(txn_id, key_space_id, key);
        }

        result
    }

    fn check_is_valid_txn(&self, txn_id: TxnId) -> Result<(), Error> {
        if self.txn_manager.is_active_txn(txn_id) {
            Ok(())
        } else {
            Err(Error::InvalidTxnId)
        }
    }

    fn commit_keys(&self, key_space_id: KeySpaceId, key_set: &HashSet<Vec<u8>>) {
        self.key_spaces
            .read()
            .expect("Could not acquire read lock for key spaces")
            .get(key_space_id)
            .expect("Invalid key space ID")
            .commit_keys(key_set)
    }

    fn abort_keys(&self, key_space_id: KeySpaceId, key_set: &HashSet<Vec<u8>>) {
        self.key_spaces
            .read()
            .expect("Could not acquire read lock for key spaces")
            .get(key_space_id)
            .expect("Invalid key space ID")
            .abort_keys(key_set)
    }
}
