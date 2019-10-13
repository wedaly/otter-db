use crate::kvs;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    KvsError(kvs::Error),
    DatabaseAlreadyExists,
    DatabaseDoesNotExist,
    TableAlreadyExists,
    TableDoesNotExist,
    ColumnAlreadyExists,
    ColumnDoesNotExist,
}

impl From<kvs::Error> for Error {
    fn from(err: kvs::Error) -> Error {
        Error::KvsError(err)
    }
}
