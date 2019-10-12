use crate::kvs::Key as KvsKey;
use crate::kvs::KeySpaceId as KvsKeySpace;

#[derive(Hash, Eq, PartialEq, Clone, Copy)]
pub enum KeySpace {
    Catalog,
}

impl KvsKeySpace for KeySpace {}

#[derive(Hash, Eq, PartialEq, Clone)]
pub enum Key {
    SystemMeta,
    DatabaseMeta { db: String },
    TableMeta { db: String, tbl: String },
}

impl KvsKey for Key {}
