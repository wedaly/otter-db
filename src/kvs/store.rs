use crate::encode::{Decode, Encode};
use crate::kvs::error::Error;
use crate::kvs::key::Key;
use crate::kvs::keyspace::{KeySpace, KeySpaceId};
use crate::kvs::txn::{TxnId, TxnManager};
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
        let mut keyspace_map = self
            .keyspace_map
            .write()
            .expect("Could not acquire write lock on keyspace map");

        keyspace_map
            .entry(keyspace_id)
            .or_insert_with(KeySpace::new);
    }

    /// Execute `f` within a transaction, committing on success
    /// and aborting on failure.  The function `f` should NOT
    /// commit the txn, abort the txn, begin a new txn, or call `with_txn()`.
    pub fn with_txn<F, R, E>(&self, mut f: F) -> Result<R, E>
    where
        E: From<Error>,
        F: FnMut(TxnId) -> Result<R, E>,
    {
        let txn_id = self.begin_txn();
        match f(txn_id) {
            Ok(result) => {
                self.commit_txn(txn_id)?;
                Ok(result)
            }
            Err(err) => {
                self.abort_txn(txn_id)?;
                Err(err)
            }
        }
    }

    pub fn get<V>(&self, txn_id: TxnId, keyspace_id: S, key: &K) -> Result<Option<V>, Error>
    where
        V: Decode,
    {
        self.check_is_valid_txn(txn_id)?;
        self.keyspace_map
            .read()
            .expect("Could not acquire read lock on keyspace map")
            .get(&keyspace_id)
            .ok_or(Error::UndefinedKeySpace)
            .and_then(|ks| ks.get(txn_id, key))
            .and_then(|v| {
                self.txn_manager.record_read(txn_id, keyspace_id, key);
                Ok(v)
            })
    }

    pub fn set<V>(&self, txn_id: TxnId, keyspace_id: S, key: &K, val: &V) -> Result<(), Error>
    where
        V: Encode,
    {
        self.check_is_valid_txn(txn_id)?;
        self.keyspace_map
            .read()
            .expect("Could not acquire read lock on keyspace map")
            .get(&keyspace_id)
            .ok_or(Error::UndefinedKeySpace)
            .and_then(|ks| ks.set(txn_id, key, val))
            .and_then(|_| {
                self.txn_manager.record_write(txn_id, keyspace_id, key);
                Ok(())
            })
    }

    pub fn delete(&self, txn_id: TxnId, keyspace_id: S, key: &K) -> Result<(), Error> {
        self.check_is_valid_txn(txn_id)?;
        self.keyspace_map
            .read()
            .expect("Could not acquire read lock on keyspace map")
            .get(&keyspace_id)
            .ok_or(Error::UndefinedKeySpace)
            .and_then(|ks| ks.delete(txn_id, key))
            .and_then(|_| {
                self.txn_manager.record_write(txn_id, keyspace_id, key);
                Ok(())
            })
    }

    fn begin_txn(&self) -> TxnId {
        self.txn_manager.begin_txn()
    }

    fn commit_txn(&self, txn_id: TxnId) -> Result<(), Error> {
        self.txn_manager.commit_txn(
            txn_id,
            |keyspace_id, key_set| self.commit_keys(keyspace_id, key_set),
            |keyspace_id, key_set| self.abort_keys(keyspace_id, key_set),
        )
    }

    fn abort_txn(&self, txn_id: TxnId) -> Result<(), Error> {
        self.txn_manager.abort_txn(txn_id, |keyspace_id, key_set| {
            self.abort_keys(keyspace_id, key_set)
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Eq, PartialEq, Clone, Copy, Hash)]
    pub struct TestKeySpace {}
    impl KeySpaceId for TestKeySpace {}

    enum Step {
        Set {
            txn_id: TxnId,
            key: &'static str,
            val: &'static str,
            expect: Result<(), Error>,
        },
        Del {
            txn_id: TxnId,
            key: &'static str,
            expect: Result<(), Error>,
        },
        Get {
            txn_id: TxnId,
            key: &'static str,
            expect: Result<Option<String>, Error>,
        },
        BeginTxn {
            expect: TxnId,
        },
        CommitTxn {
            txn_id: TxnId,
            expect: Result<(), Error>,
        },
        AbortTxn {
            txn_id: TxnId,
            expect: Result<(), Error>,
        },
    }

    fn run_test(mut steps: Vec<Step>) {
        let store = Store::new();
        store.define_keyspace(TestKeySpace {});

        for step in steps.drain(..) {
            match step {
                Step::Set {
                    txn_id,
                    key,
                    val,
                    expect,
                } => {
                    let result = store.set(txn_id, TestKeySpace {}, &key, &val);
                    assert_eq!(result, expect);
                }
                Step::Del {
                    txn_id,
                    key,
                    expect,
                } => {
                    let result = store.delete(txn_id, TestKeySpace {}, &key);
                    assert_eq!(result, expect);
                }
                Step::Get {
                    txn_id,
                    key,
                    expect,
                } => {
                    let result = store.get(txn_id, TestKeySpace {}, &key);
                    assert_eq!(result, expect);
                }
                Step::BeginTxn { expect } => {
                    let result = store.begin_txn();
                    assert_eq!(result, expect);
                }
                Step::CommitTxn { txn_id, expect } => {
                    let result = store.commit_txn(txn_id);
                    assert_eq!(result, expect);
                }
                Step::AbortTxn { txn_id, expect } => {
                    let result = store.abort_txn(txn_id);
                    assert_eq!(result, expect);
                }
            }
        }
    }

    #[test]
    fn test_get_invalid_txn() {
        run_test(vec![Step::Get {
            txn_id: 1234,
            key: "abcd1234",
            expect: Err(Error::InvalidTxnId),
        }])
    }

    #[test]
    fn test_set_invalid_txn() {
        run_test(vec![Step::Set {
            txn_id: 1234,
            key: "abcd1234",
            val: "xyz56789",
            expect: Err(Error::InvalidTxnId),
        }])
    }

    #[test]
    fn test_del_invalid_txn() {
        run_test(vec![Step::Del {
            txn_id: 1234,
            key: "abcd1234",
            expect: Err(Error::InvalidTxnId),
        }])
    }

    #[test]
    fn test_commit_invalid_txn() {
        run_test(vec![Step::CommitTxn {
            txn_id: 1234,
            expect: Err(Error::InvalidTxnId),
        }])
    }

    #[test]
    fn test_abort_invalid_txn() {
        run_test(vec![Step::AbortTxn {
            txn_id: 1234,
            expect: Err(Error::InvalidTxnId),
        }])
    }

    #[test]
    fn test_commit_no_changes_sequential() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 4 },
            Step::CommitTxn {
                txn_id: 4,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_commit_no_changes_interleaved() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::BeginTxn { expect: 1 },
            Step::BeginTxn { expect: 2 },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 1,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_abort_no_changes() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::AbortTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 1 },
            Step::AbortTxn {
                txn_id: 1,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::AbortTxn {
                txn_id: 2,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_abort_no_changes_interleaved() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::BeginTxn { expect: 1 },
            Step::BeginTxn { expect: 2 },
            Step::AbortTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::AbortTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::AbortTxn {
                txn_id: 1,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_reuse_committed_txn() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::Get {
                txn_id: 0,
                key: "abcd1234",
                expect: Err(Error::InvalidTxnId),
            },
        ])
    }

    #[test]
    fn test_reuse_aborted_txn() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::AbortTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::Get {
                txn_id: 0,
                key: "abcd1234",
                expect: Err(Error::InvalidTxnId),
            },
        ])
    }

    #[test]
    fn test_get_does_not_exist() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Get {
                txn_id: 0,
                key: "abcd1234",
                expect: Ok(None),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_set_and_get_uncommitted_same_txn() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::Get {
                txn_id: 0,
                key: "foo",
                expect: Ok(Some("bar".to_string())),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_insert_and_get_committed() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::Get {
                txn_id: 2,
                key: "foo",
                expect: Ok(Some("bar".to_string())),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_update_and_get_committed() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "updated",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::Get {
                txn_id: 2,
                key: "foo",
                expect: Ok(Some("updated".to_string())),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_del_does_not_exist() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Del {
                txn_id: 0,
                key: "foo",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_del_uncommitted_same_txn() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::Del {
                txn_id: 0,
                key: "foo",
                expect: Ok(()),
            },
            Step::Get {
                txn_id: 0,
                key: "foo",
                expect: Ok(None),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_del_committed() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::Del {
                txn_id: 2,
                key: "foo",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 4 },
            Step::Get {
                txn_id: 4,
                key: "foo",
                expect: Ok(None),
            },
            Step::CommitTxn {
                txn_id: 4,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_uncommitted_update_visibility() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::Set {
                txn_id: 2,
                key: "foo",
                val: "updated",
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 3 },
            Step::Get {
                txn_id: 3,
                key: "foo",
                expect: Ok(Some("bar".to_string())),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_uncommitted_del_visibility() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::Del {
                txn_id: 2,
                key: "foo",
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 3 },
            Step::Get {
                txn_id: 3,
                key: "foo",
                expect: Ok(Some("bar".to_string())),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_get_set_read_write_conflict() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::BeginTxn { expect: 3 },
            Step::Get {
                txn_id: 3,
                key: "foo",
                expect: Ok(Some("bar".to_string())),
            },
            Step::Set {
                txn_id: 2,
                key: "foo",
                val: "updated",
                expect: Err(Error::ReadWriteConflict),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_get_del_read_write_conflict() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::BeginTxn { expect: 3 },
            Step::Get {
                txn_id: 3,
                key: "foo",
                expect: Ok(Some("bar".to_string())),
            },
            Step::Del {
                txn_id: 2,
                key: "foo",
                expect: Err(Error::ReadWriteConflict),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_set_write_conflict() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::Set {
                txn_id: 2,
                key: "foo",
                val: "updated",
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 3 },
            Step::Set {
                txn_id: 3,
                key: "foo",
                val: "conflict",
                expect: Err(Error::WriteWriteConflict),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_del_write_conflict() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::Set {
                txn_id: 2,
                key: "foo",
                val: "updated",
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 3 },
            Step::Del {
                txn_id: 3,
                key: "foo",
                expect: Err(Error::WriteWriteConflict),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_commit_multiple_changes() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::Set {
                txn_id: 0,
                key: "baa",
                val: "bit",
                expect: Ok(()),
            },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "baz",
                expect: Ok(()),
            },
            Step::Set {
                txn_id: 0,
                key: "baa",
                val: "biz",
                expect: Ok(()),
            },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bing",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::Get {
                txn_id: 2,
                key: "foo",
                expect: Ok(Some("bing".to_string())),
            },
            Step::Get {
                txn_id: 2,
                key: "baa",
                expect: Ok(Some("biz".to_string())),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_phantom_insert_then_read_validation() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::BeginTxn { expect: 1 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "phantom",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::Get {
                txn_id: 1,
                key: "foo",
                expect: Ok(Some("phantom".to_string())),
            },
            Step::CommitTxn {
                txn_id: 1,
                expect: Err(Error::PhantomDetected),
            },
        ])
    }

    #[test]
    fn test_phantom_read_then_insert_validation() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::BeginTxn { expect: 1 },
            Step::Get {
                txn_id: 1,
                key: "foo",
                expect: Ok(None),
            },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "phantom",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 1,
                expect: Err(Error::PhantomDetected),
            },
        ])
    }

    #[test]
    fn test_phantom_update_validation() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::BeginTxn { expect: 3 },
            Step::Set {
                txn_id: 2,
                key: "foo",
                val: "phantom",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::Get {
                txn_id: 3,
                key: "foo",
                expect: Ok(Some("phantom".to_string())),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Err(Error::PhantomDetected),
            },
        ])
    }

    #[test]
    fn test_phantom_del_validation() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::BeginTxn { expect: 3 },
            Step::Del {
                txn_id: 2,
                key: "foo",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::Get {
                txn_id: 3,
                key: "foo",
                expect: Ok(None),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Err(Error::PhantomDetected),
            },
        ])
    }

    #[test]
    fn test_phantom_insert_and_del_validation() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::BeginTxn { expect: 1 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "phantom",
                expect: Ok(()),
            },
            Step::Del {
                txn_id: 0,
                key: "foo",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::Get {
                txn_id: 1,
                key: "foo",
                expect: Ok(None),
            },
            Step::CommitTxn {
                txn_id: 1,
                expect: Err(Error::PhantomDetected),
            },
        ])
    }

    #[test]
    fn test_failed_commit_reverts_insert() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::BeginTxn { expect: 1 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "phantom",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::Get {
                txn_id: 1,
                key: "foo",
                expect: Ok(Some("phantom".to_string())),
            },
            Step::Set {
                txn_id: 1,
                key: "bar",
                val: "revert",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 1,
                expect: Err(Error::PhantomDetected),
            },
            Step::BeginTxn { expect: 3 },
            Step::Get {
                txn_id: 3,
                key: "revert",
                expect: Ok(None),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_failed_commit_reverts_update() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::BeginTxn { expect: 3 },
            Step::Set {
                txn_id: 2,
                key: "foo",
                val: "phantom",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::Get {
                txn_id: 3,
                key: "foo",
                expect: Ok(Some("phantom".to_string())),
            },
            Step::Set {
                txn_id: 3,
                key: "foo",
                val: "revert",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Err(Error::PhantomDetected),
            },
            Step::BeginTxn { expect: 5 },
            Step::Get {
                txn_id: 5,
                key: "foo",
                expect: Ok(Some("phantom".to_string())),
            },
            Step::CommitTxn {
                txn_id: 5,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_failed_commit_reverts_delete() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::BeginTxn { expect: 3 },
            Step::Del {
                txn_id: 2,
                key: "foo",
                expect: Ok(()),
            },
            Step::Set {
                txn_id: 2,
                key: "bar",
                val: "revert",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::Get {
                txn_id: 3,
                key: "foo",
                expect: Ok(None),
            },
            Step::Del {
                txn_id: 3,
                key: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Err(Error::PhantomDetected),
            },
            Step::BeginTxn { expect: 5 },
            Step::Get {
                txn_id: 5,
                key: "bar",
                expect: Ok(Some("revert".to_string())),
            },
            Step::CommitTxn {
                txn_id: 5,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_abort_insert() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::AbortTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 1 },
            Step::Get {
                txn_id: 1,
                key: "foo",
                expect: Ok(None),
            },
            Step::CommitTxn {
                txn_id: 1,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_abort_update() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::Set {
                txn_id: 2,
                key: "foo",
                val: "updated",
                expect: Ok(()),
            },
            Step::AbortTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 3 },
            Step::Get {
                txn_id: 3,
                key: "foo",
                expect: Ok(Some("bar".to_string())),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_abort_del() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::Del {
                txn_id: 2,
                key: "foo",
                expect: Ok(()),
            },
            Step::AbortTxn {
                txn_id: 2,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 3 },
            Step::Get {
                txn_id: 3,
                key: "foo",
                expect: Ok(Some("bar".to_string())),
            },
            Step::CommitTxn {
                txn_id: 3,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_abort_multiple_changes() {
        run_test(vec![
            Step::BeginTxn { expect: 0 },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bar",
                expect: Ok(()),
            },
            Step::Set {
                txn_id: 0,
                key: "baa",
                val: "bit",
                expect: Ok(()),
            },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "baz",
                expect: Ok(()),
            },
            Step::Set {
                txn_id: 0,
                key: "baa",
                val: "biz",
                expect: Ok(()),
            },
            Step::Set {
                txn_id: 0,
                key: "foo",
                val: "bing",
                expect: Ok(()),
            },
            Step::CommitTxn {
                txn_id: 0,
                expect: Ok(()),
            },
            Step::BeginTxn { expect: 2 },
            Step::Get {
                txn_id: 2,
                key: "foo",
                expect: Ok(Some("bing".to_string())),
            },
            Step::Get {
                txn_id: 2,
                key: "baa",
                expect: Ok(Some("biz".to_string())),
            },
            Step::CommitTxn {
                txn_id: 2,
                expect: Ok(()),
            },
        ])
    }

    #[test]
    fn test_with_txn_success() {
        let store = Store::new();
        store.define_keyspace(TestKeySpace {});

        let (key, val) = ("foo", "bar");
        let r1: Result<(), Error> =
            store.with_txn(|txn_id| store.set(txn_id, TestKeySpace {}, &key, &val));
        assert_eq!(r1, Ok(()));

        let r2: Result<Option<String>, Error> =
            store.with_txn(|txn_id| store.get(txn_id, TestKeySpace {}, &key));
        assert_eq!(r2, Ok(Some(val.to_string())));
    }

    #[test]
    fn test_with_txn_failure() {
        let store = Store::new();
        store.define_keyspace(TestKeySpace {});

        let (key, val) = ("foo", "bar");
        let r1: Result<(), Error> = store.with_txn(|txn_id| {
            store
                .set(txn_id, TestKeySpace {}, &key, &val)
                .expect("Could not set key");
            Err(Error::WriteWriteConflict)
        });
        assert_eq!(r1, Err(Error::WriteWriteConflict));

        let r2: Result<Option<String>, Error> =
            store.with_txn(|txn_id| store.get(txn_id, TestKeySpace {}, &key));
        assert_eq!(r2, Ok(None));
    }
}
