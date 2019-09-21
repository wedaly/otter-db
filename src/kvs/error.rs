use crate::kvs::value::DeserializationError;

#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    UndefinedKeySpace,
    VersionNotFound,
    InvalidTxnId,
    ReadWriteConflict,
    WriteWriteConflict,
    PhantomDetected,
    DeserializationError(DeserializationError),
}

impl From<DeserializationError> for Error {
    fn from(err: DeserializationError) -> Error {
        Error::DeserializationError(err)
    }
}
