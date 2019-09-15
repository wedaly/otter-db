#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    UndefinedKeySpace,
    VersionNotFound,
    InvalidTxnId,
    ReadWriteConflict,
    WriteWriteConflict,
    PhantomDetected,
}
