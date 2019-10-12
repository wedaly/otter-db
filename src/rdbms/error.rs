use crate::kvs::Error as KvsError;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    KvsError(KvsError),
    DatabaseAlreadyExists,
    DatabaseDoesNotExist,
    TableAlreadyExists,
    TableDoesNotExist,
}

impl From<KvsError> for Error {
    fn from(err: KvsError) -> Error {
        Error::KvsError(err)
    }
}
