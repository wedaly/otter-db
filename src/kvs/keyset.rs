use crate::kvs::key::Key;
use crate::kvs::keyspace::KeySpaceId;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

pub struct KeySet<S, K>
where
    S: KeySpaceId,
    K: Key,
{
    keyspace_map: Mutex<HashMap<S, HashSet<K>>>,
}

impl<S, K> KeySet<S, K>
where
    S: KeySpaceId,
    K: Key,
{
    pub fn new() -> KeySet<S, K> {
        KeySet {
            keyspace_map: Mutex::new(HashMap::new()),
        }
    }

    pub fn add_key(&self, keyspace_id: S, key: &K) {
        let mut keyspace_map = self
            .keyspace_map
            .lock()
            .expect("Could not acquire lock on key space map");

        keyspace_map
            .entry(keyspace_id)
            .and_modify(|set| {
                set.insert(key.clone());
            })
            .or_insert_with(|| {
                let mut set = HashSet::new();
                set.insert(key.clone());
                set
            });
    }

    pub fn for_each_keyspace_keys<F>(&self, mut f: F)
    where
        F: FnMut(S, &HashSet<K>),
    {
        let keyspace_map = self
            .keyspace_map
            .lock()
            .expect("Could not acquire lock on key space map");

        for (keyspace_id, keyset) in keyspace_map.iter() {
            f(*keyspace_id, keyset)
        }
    }

    pub fn overlaps(&self, other: &KeySet<S, K>) -> bool {
        let keyspace_map = self
            .keyspace_map
            .lock()
            .expect("Could not acquire lock on key space map");

        for (keyspace_id, keyset) in keyspace_map.iter() {
            let other_keyspace_map = other
                .keyspace_map
                .lock()
                .expect("Could not acquire lock on other keyspace map");
            if let Some(other_keyset) = other_keyspace_map.get(keyspace_id) {
                if !keyset.is_disjoint(other_keyset) {
                    return true;
                }
            }
        }

        return false;
    }
}
