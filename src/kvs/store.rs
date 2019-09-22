use crate::kvs::error::Error;
use crate::kvs::key::Key;
use crate::kvs::keyspace::{KeySpace, KeySpaceId};
use crate::kvs::txn::{TxnId, TxnManager};
use crate::kvs::value::{DeserializableValue, SerializableValue};
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

pub struct Store<S, K>
where
    S: KeySpaceId,
    K: Key,
{
    txn_manager: TxnManager<S, K>,
    keyspace_map: RwLock<HashMap<S, KeySpace<K>>>,
}

impl<S, K> Store<S, K>
where
    S: KeySpaceId,
    K: Key,
{
    pub fn new() -> Store<S, K> {
        Store {
            txn_manager: TxnManager::new(),
            keyspace_map: RwLock::new(HashMap::new()),
        }
    }

    pub fn define_keyspace(&self, keyspace_id: S) {
        self.keyspace_map
            .write()
            .expect("Could not acquire write lock on keyspace map")
            .insert(keyspace_id, KeySpace::new());
    }

    pub fn begin_txn(&self) -> TxnId {
        self.txn_manager.begin_txn()
    }

    pub fn commit_txn(&self, txn_id: TxnId) -> Result<(), Error> {
        self.txn_manager.commit_txn(
            txn_id,
            |keyspace_id, key_set| self.commit_keys(keyspace_id, key_set),
            |keyspace_id, key_set| self.abort_keys(keyspace_id, key_set),
        )
    }

    pub fn abort_txn(&self, txn_id: TxnId) -> Result<(), Error> {
        self.txn_manager.abort_txn(txn_id, |keyspace_id, key_set| {
            self.abort_keys(keyspace_id, key_set)
        })
    }

    pub fn get<V>(&self, txn_id: TxnId, keyspace_id: S, key: &K) -> Result<Option<V>, Error>
    where
        V: DeserializableValue,
    {
        self.check_is_valid_txn(txn_id)?;
        let result = self
            .keyspace_map
            .read()
            .expect("Could not acquire read lock on keyspace map")
            .get(&keyspace_id)
            .ok_or(Error::UndefinedKeySpace)
            .and_then(|ks| ks.get(txn_id, key));

        if result.is_ok() {
            self.txn_manager.record_read(txn_id, keyspace_id, key);
        }

        result
    }

    pub fn set<V>(&self, txn_id: TxnId, keyspace_id: S, key: &K, val: &V) -> Result<(), Error>
    where
        V: SerializableValue,
    {
        self.check_is_valid_txn(txn_id)?;
        let result = self
            .keyspace_map
            .read()
            .expect("Could not acquire read lock on keyspace map")
            .get(&keyspace_id)
            .ok_or(Error::UndefinedKeySpace)
            .and_then(|ks| ks.set(txn_id, key, val));

        if result.is_ok() {
            self.txn_manager.record_write(txn_id, keyspace_id, key);
        }

        result
    }

    pub fn delete(&self, txn_id: TxnId, keyspace_id: S, key: &K) -> Result<(), Error> {
        self.check_is_valid_txn(txn_id)?;
        let result = self
            .keyspace_map
            .read()
            .expect("Could not acquire read lock on keyspace map")
            .get(&keyspace_id)
            .ok_or(Error::UndefinedKeySpace)
            .and_then(|ks| ks.delete(txn_id, key));

        if result.is_ok() {
            self.txn_manager.record_write(txn_id, keyspace_id, key);
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

    fn commit_keys(&self, keyspace_id: S, key_set: &HashSet<K>) {
        self.keyspace_map
            .read()
            .expect("Could not acquire read lock on keyspace map")
            .get(&keyspace_id)
            .expect("Invalid key space ID")
            .commit_keys(key_set)
    }

    fn abort_keys(&self, keyspace_id: S, key_set: &HashSet<K>) {
        self.keyspace_map
            .read()
            .expect("Could not acquire read lock on keyspace map")
            .get(&keyspace_id)
            .expect("Invalid key space ID")
            .abort_keys(key_set)
    }
}
