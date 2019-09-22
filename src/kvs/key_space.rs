use crate::kvs::error::Error;
use crate::kvs::key::Key;
use crate::kvs::txn::TxnId;
use crate::kvs::value::{DeserializableValue, SerializableValue};
use crate::kvs::version::{Version, VersionId, VersionTable};
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

pub type KeySpaceId = usize;

pub struct KeySpace<K>
where
    K: Key,
{
    key_map: RwLock<HashMap<K, VersionId>>,
    version_tbl: VersionTable,
}

impl<K> KeySpace<K>
where
    K: Key,
{
    pub fn new() -> KeySpace<K> {
        KeySpace {
            key_map: RwLock::new(HashMap::new()),
            version_tbl: VersionTable::new(),
        }
    }

    pub fn get<V>(&self, txn_id: TxnId, key: &K) -> Result<Option<V>, Error>
    where
        K: Key,
        V: DeserializableValue,
    {
        let key_map = self
            .key_map
            .read()
            .expect("Could not acquire read lock for key map");
        match key_map.get(key) {
            None => Ok(None),
            Some(version_id) => {
                let val_opt = self.version_tbl.retrieve(txn_id, *version_id)?;
                Ok(val_opt)
            }
        }
    }

    pub fn set<V>(&self, txn_id: TxnId, key: &K, val: &V) -> Result<(), Error>
    where
        V: SerializableValue,
    {
        self.upsert_uncommitted_version(txn_id, key, Version::Value(val))
    }

    pub fn delete(&self, txn_id: TxnId, key: &K) -> Result<(), Error> {
        self.upsert_uncommitted_version::<&[u8]>(txn_id, key, Version::Deleted)
    }

    pub fn commit_keys(&self, key_set: &HashSet<K>) {
        let key_map = self
            .key_map
            .read()
            .expect("Could not acquire read lock for key map");

        for key in key_set.iter() {
            let version_id = key_map.get(key).expect("Could not find key");
            self.version_tbl.commit(*version_id);
        }
    }

    pub fn abort_keys(&self, key_set: &HashSet<K>) {
        let mut key_map = self
            .key_map
            .write()
            .expect("Could not acquire write lock for key map");

        for key in key_set.iter() {
            let version_id = key_map.get(key).expect("Could not find key");
            match self.version_tbl.abort(*version_id) {
                None => {
                    key_map.remove(key);
                }
                Some(prev_version_id) => {
                    key_map.insert(key.clone(), prev_version_id);
                }
            }
        }
    }

    pub fn upsert_uncommitted_version<V>(
        &self,
        txn_id: TxnId,
        key: &K,
        version: Version<V>,
    ) -> Result<(), Error>
    where
        V: SerializableValue,
    {
        let mut key_map = self
            .key_map
            .write()
            .expect("Could not acquire write lock for key map");
        match key_map.get_mut(key) {
            None => {
                // key doesn't already exist, so insert a new version
                let version_id = self.version_tbl.append_first_version(txn_id, version);
                key_map.insert(key.clone(), version_id);
                Ok(())
            }
            Some(v) => {
                // key already exists, so insert a new version after the previous version
                let prev_version_id = *v;
                *v = self
                    .version_tbl
                    .append_next_version(txn_id, prev_version_id, version)?;
                Ok(())
            }
        }
    }
}
