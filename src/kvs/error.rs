use crate::encode::Error as EncodeError;

#[derive(Debug, Eq, PartialEq)]
pub enum Error {
    UndefinedKeySpace,
    VersionNotFound,
    InvalidTxnId,
    ReadWriteConflict,
    WriteWriteConflict,
    PhantomDetected,
    EncodeError(EncodeError),
}

impl From<EncodeError> for Error {
    fn from(err: EncodeError) -> Error {
        Error::EncodeError(err)
    }
}
