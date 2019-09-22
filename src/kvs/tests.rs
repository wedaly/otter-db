use crate::kvs::error::Error;
use crate::kvs::keyspace::GLOBAL_KEYSPACE;
use crate::kvs::store::Store;
use crate::kvs::txn::TxnId;

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
    store.define_keyspace(GLOBAL_KEYSPACE);

    for step in steps.drain(..) {
        match step {
            Step::Set {
                txn_id,
                key,
                val,
                expect,
            } => {
                let result = store.set(txn_id, GLOBAL_KEYSPACE, &key, &val);
                assert_eq!(result, expect);
            }
            Step::Del {
                txn_id,
                key,
                expect,
            } => {
                let result = store.delete(txn_id, GLOBAL_KEYSPACE, &key);
                assert_eq!(result, expect);
            }
            Step::Get {
                txn_id,
                key,
                expect,
            } => {
                let result = store.get(txn_id, GLOBAL_KEYSPACE, &key);
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
