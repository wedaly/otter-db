use crate::kvs::error::Error;
use crate::kvs::txn::TxnId;
use crate::kvs::version::{VersionData, VersionId, VersionTable};
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

pub type KeySpaceId = usize;

pub struct KeySpace {
    key_map: RwLock<HashMap<Vec<u8>, VersionId>>,
    version_tbl: VersionTable,
}

impl KeySpace {
    pub fn new() -> KeySpace {
        KeySpace {
            key_map: RwLock::new(HashMap::new()),
            version_tbl: VersionTable::new(),
        }
    }

    pub fn get(&self, txn_id: TxnId, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let key_map = self
            .key_map
            .read()
            .expect("Could not acquire read lock for key map");
        let val_opt = key_map
            .get(key)
            .and_then(|version_id| self.version_tbl.retrieve(txn_id, *version_id));
        Ok(val_opt)
    }

    pub fn set(&self, txn_id: TxnId, key: &[u8], val: Vec<u8>) -> Result<(), Error> {
        self.upsert_uncommitted_version(txn_id, key, Some(val))
    }

    pub fn delete(&self, txn_id: TxnId, key: &[u8]) -> Result<(), Error> {
        self.upsert_uncommitted_version(txn_id, key, None)
    }

    pub fn commit_keys(&self, key_set: &HashSet<Vec<u8>>) {
        let key_map = self
            .key_map
            .read()
            .expect("Could not acquire read lock for key map");

        for key in key_set.iter() {
            let version_id = key_map.get(key).expect("Could not find key");
            self.version_tbl.commit(*version_id);
        }
    }

    pub fn abort_keys(&self, key_set: &HashSet<Vec<u8>>) {
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
                    key_map.insert(key.to_vec(), prev_version_id);
                }
            }
        }
    }

    pub fn upsert_uncommitted_version(
        &self,
        txn_id: TxnId,
        key: &[u8],
        data: VersionData,
    ) -> Result<(), Error> {
        let mut key_map = self
            .key_map
            .write()
            .expect("Could not acquire write lock for key map");
        match key_map.get_mut(key) {
            None => {
                // key doesn't already exist, so insert a new version
                let version_id = self.version_tbl.append_first_version(txn_id, data);
                key_map.insert(key.to_vec(), version_id);
                Ok(())
            }
            Some(v) => {
                // key already exists, so insert a new version after the previous version
                let prev_version_id = *v;
                *v = self
                    .version_tbl
                    .append_next_version(txn_id, prev_version_id, data)?;
                Ok(())
            }
        }
    }
}
