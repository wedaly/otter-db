use crate::kvs;

#[derive(Hash, Eq, PartialEq, Clone, Copy)]
pub enum KeySpace {
    Catalog,
}

impl kvs::KeySpaceId for KeySpace {}

#[derive(Hash, Eq, PartialEq, Clone)]
pub enum Key {
    SystemMeta,
    DatabaseMeta {
        db: String,
    },
    TableMeta {
        db: String,
        tbl: String,
    },
    ColumnMeta {
        db: String,
        tbl: String,
        col: String,
    },
}

impl kvs::Key for Key {}
