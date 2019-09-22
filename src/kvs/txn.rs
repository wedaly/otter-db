use crate::kvs::error::Error;
use crate::kvs::key::Key;
use crate::kvs::keyset::KeySet;
use crate::kvs::keyspace::KeySpaceId;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

pub type TxnId = usize;

struct Txn<S, K>
where
    S: KeySpaceId,
    K: Key,
{
    write_set: KeySet<S, K>,
    read_set: KeySet<S, K>,
}

pub struct TxnManager<S, K>
where
    S: KeySpaceId,
    K: Key,
{
    next_txn_id: AtomicUsize,
    active_txns: RwLock<BTreeMap<TxnId, Txn<S, K>>>,
    recently_committed_txns: Mutex<HashMap<TxnId, Txn<S, K>>>,
}

impl<S, K> TxnManager<S, K>
where
    S: KeySpaceId,
    K: Key,
{
    pub fn new() -> TxnManager<S, K> {
        TxnManager {
            next_txn_id: AtomicUsize::new(0),
            active_txns: RwLock::new(BTreeMap::new()),
            recently_committed_txns: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_active_txn(&self, txn_id: TxnId) -> bool {
        self.active_txns
            .read()
            .expect("Could not acquire read lock on active transactions map")
            .contains_key(&txn_id)
    }

    pub fn begin_txn(&self) -> TxnId {
        let txn_id = self.get_next_txn_id();

        self.active_txns
            .write()
            .expect("Could not acquire write lock on active transactions map")
            .insert(
                txn_id,
                Txn {
                    write_set: KeySet::new(),
                    read_set: KeySet::new(),
                },
            );

        txn_id
    }

    pub fn commit_txn<F, G>(
        &self,
        txn_id: TxnId,
        commit_keys: F,
        abort_keys: G,
    ) -> Result<(), Error>
    where
        F: FnMut(S, &HashSet<K>),
        G: FnMut(S, &HashSet<K>),
    {
        // Hold exclusive locks on the active transactions map
        // and the recently committed transactions map for the duration
        // of the commit operation.
        // In effect, this serializes the commit operations.
        let mut active_txns = self
            .active_txns
            .write()
            .expect("Could not acquire write lock on active transactions map");

        let mut recently_committed_txns = self
            .recently_committed_txns
            .lock()
            .expect("Could not acquire write lock on recently committed txns");

        let txn = active_txns.remove(&txn_id).ok_or(Error::InvalidTxnId)?;
        let begin_ts = txn_id;
        let min_active_txn_id = active_txns.keys().min();
        let mut discard_txns = Vec::new();

        for (committed_txn_id, committed_txn) in recently_committed_txns.iter() {
            // If another txn wrote a key that this txn read,
            // it could cause a phantom anomaly, so we abort the txn.
            if *committed_txn_id > begin_ts {
                if txn.read_set.overlaps(&committed_txn.write_set) {
                    txn.write_set.for_each_keyspace_keys(abort_keys);
                    return Err(Error::PhantomDetected);
                }
            }

            // If a recently committed txn has a timestamp before
            // the oldest active txn, then it can never conflict
            // with an active txn, so we can discard it.
            if let Some(min) = min_active_txn_id {
                if *committed_txn_id < *min {
                    discard_txns.push(*committed_txn_id);
                }
            }
        }

        for txn_id in discard_txns.iter() {
            recently_committed_txns.remove(txn_id);
        }

        // Validation passed, so commit the changes
        let commit_ts = self.get_next_txn_id();
        txn.write_set.for_each_keyspace_keys(commit_keys);
        recently_committed_txns.insert(commit_ts, txn);

        Ok(())
    }

    pub fn abort_txn<F>(&self, txn_id: TxnId, abort_keys: F) -> Result<(), Error>
    where
        F: FnMut(S, &HashSet<K>),
    {
        let mut active_txns = self
            .active_txns
            .write()
            .expect("Could not acquire write lock on active transactions map");
        let txn = active_txns.remove(&txn_id).ok_or(Error::InvalidTxnId)?;
        txn.write_set.for_each_keyspace_keys(abort_keys);
        Ok(())
    }

    pub fn record_write(&self, txn_id: TxnId, keyspace_id: S, key: &K) {
        self.run_on_txn(txn_id, |txn| txn.write_set.add_key(keyspace_id, key))
    }

    pub fn record_read(&self, txn_id: TxnId, keyspace_id: S, key: &K) {
        self.run_on_txn(txn_id, |txn| txn.read_set.add_key(keyspace_id, key))
    }

    fn get_next_txn_id(&self) -> usize {
        self.next_txn_id.fetch_add(1, Ordering::SeqCst)
    }

    fn run_on_txn<F>(&self, txn_id: TxnId, mut f: F)
    where
        F: FnMut(&Txn<S, K>),
    {
        let active_txns = self
            .active_txns
            .read()
            .expect("Could not acquire read lock on active transaction map");

        let txn = active_txns
            .get(&txn_id)
            .expect("Could not find active transaction");

        f(txn)
    }
}
