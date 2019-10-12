use crate::kvs::Store;
use crate::kvs::TxnId;
use crate::rdbms::catalog::database_meta::DatabaseMeta;
use crate::rdbms::catalog::system_meta::SystemMeta;
use crate::rdbms::catalog::table_meta::TableMeta;
use crate::rdbms::error::Error;
use crate::rdbms::key::{Key, KeySpace};

pub struct Catalog<'a> {
    store: &'a Store<KeySpace, Key>,
}

impl<'a> Catalog<'a> {
    pub fn new(store: &'a Store<KeySpace, Key>) -> Catalog {
        store.define_keyspace(KeySpace::Catalog);
        Catalog { store }
    }

    pub fn create_database(&self, db_name: &str) -> Result<(), Error> {
        self.store.with_txn(|txn_id| {
            let db_meta_key = Key::DatabaseMeta {
                db: db_name.to_string(),
            };
            let db_meta_opt =
                self.store
                    .get::<DatabaseMeta>(txn_id, KeySpace::Catalog, &db_meta_key)?;

            if let Some(_) = db_meta_opt {
                return Err(Error::DatabaseAlreadyExists);
            }

            self.store.set(
                txn_id,
                KeySpace::Catalog,
                &db_meta_key,
                &DatabaseMeta::new(),
            )?;

            let mut system_meta: SystemMeta = self.get_or_create_system_meta(txn_id)?;

            system_meta.insert_db_name(db_name);
            self.store
                .set(txn_id, KeySpace::Catalog, &Key::SystemMeta, &system_meta)?;
            Ok(())
        })
    }

    pub fn create_table(&self, db_name: &str, tbl_name: &str) -> Result<(), Error> {
        self.store.with_txn(|txn_id| {
            let tbl_meta_key = Key::TableMeta {
                db: db_name.to_string(),
                tbl: tbl_name.to_string(),
            };
            let tbl_meta_opt =
                self.store
                    .get::<TableMeta>(txn_id, KeySpace::Catalog, &tbl_meta_key)?;

            if let Some(_) = tbl_meta_opt {
                return Err(Error::TableAlreadyExists);
            }

            self.store
                .set(txn_id, KeySpace::Catalog, &tbl_meta_key, &TableMeta::new())?;

            let db_meta_key = Key::DatabaseMeta {
                db: db_name.to_string(),
            };
            let mut db_meta = self
                .store
                .get::<DatabaseMeta>(txn_id, KeySpace::Catalog, &db_meta_key)?
                .ok_or(Error::DatabaseDoesNotExist)?;
            db_meta.insert_tbl_name(tbl_name);
            self.store
                .set(txn_id, KeySpace::Catalog, &db_meta_key, &db_meta)?;
            Ok(())
        })
    }

    pub fn get_system_meta(&self) -> Result<SystemMeta, Error> {
        self.store
            .with_txn(|txn_id| self.get_or_create_system_meta(txn_id))
    }

    pub fn get_database_meta(&self, db_name: &str) -> Result<DatabaseMeta, Error> {
        self.store.with_txn(|txn_id| {
            let db_meta_key = Key::DatabaseMeta {
                db: db_name.to_string(),
            };
            self.store
                .get::<DatabaseMeta>(txn_id, KeySpace::Catalog, &db_meta_key)?
                .ok_or(Error::DatabaseDoesNotExist)
        })
    }

    pub fn get_table_meta(&self, db_name: &str, tbl_name: &str) -> Result<TableMeta, Error> {
        self.store.with_txn(|txn_id| {
            let tbl_meta_key = Key::TableMeta {
                db: db_name.to_string(),
                tbl: tbl_name.to_string(),
            };
            self.store
                .get::<TableMeta>(txn_id, KeySpace::Catalog, &tbl_meta_key)?
                .ok_or(Error::TableDoesNotExist)
        })
    }

    fn get_or_create_system_meta(&self, txn_id: TxnId) -> Result<SystemMeta, Error> {
        let system_meta = self
            .store
            .get(txn_id, KeySpace::Catalog, &Key::SystemMeta)?
            .unwrap_or_else(SystemMeta::new);
        Ok(system_meta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get_database() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        catalog
            .create_database(&db_name)
            .expect("Could not create database");
        catalog
            .get_database_meta(&db_name)
            .expect("Could not retrieve database");
    }

    #[test]
    fn test_create_database_already_exists() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        catalog
            .create_database(&db_name)
            .expect("Could not create database");
        let result = catalog.create_database(&db_name);
        assert_eq!(result, Err(Error::DatabaseAlreadyExists));
    }

    #[test]
    fn test_create_and_list_databases() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_names = vec!["testdb1", "testdb2", "testdb3"];
        for db_name in db_names.iter() {
            catalog
                .create_database(&db_name)
                .expect("Could not create database");
        }
        let system_meta = catalog
            .get_system_meta()
            .expect("Could not get system meta");
        let retrieved_db_names: Vec<String> =
            system_meta.iter_db_names().map(|s| s.to_string()).collect();
        assert_eq!(retrieved_db_names, db_names);
    }

    #[test]
    fn test_get_database_does_not_exist() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let result = catalog.get_database_meta(&"notexist");
        assert_eq!(result, Err(Error::DatabaseDoesNotExist));
    }

    #[test]
    fn test_list_database_no_entries() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let system_meta = catalog
            .get_system_meta()
            .expect("Could not get system meta");
        assert_eq!(system_meta.iter_db_names().len(), 0);
    }

    #[test]
    fn test_create_table() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let tbl_names = vec!["foo", "bar", "baz"];
        catalog
            .create_database(&db_name)
            .expect("Could not create database");

        for t in tbl_names.iter() {
            catalog
                .create_table(&db_name, &t)
                .expect("Could not create table");
        }

        for t in tbl_names.iter() {
            catalog
                .get_table_meta(db_name, t)
                .expect("Could not get table");
        }
    }

    #[test]
    fn test_create_table_database_does_not_exist() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let result = catalog.create_table(&"notexists", &"foo");
        assert_eq!(result, Err(Error::DatabaseDoesNotExist));
    }

    #[test]
    fn test_create_table_already_exists() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let tbl_name = "testtbl";
        catalog
            .create_database(&db_name)
            .expect("Could not create database");

        catalog
            .create_table(&db_name, &tbl_name)
            .expect("Could not create table");
        let result = catalog.create_table(&db_name, &tbl_name);
        assert_eq!(result, Err(Error::TableAlreadyExists));
    }

    #[test]
    fn test_list_database_tables() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let mut tbl_names = vec!["foo", "bar", "baz"];
        catalog
            .create_database(&db_name)
            .expect("Could not create database");

        for t in tbl_names.iter() {
            catalog
                .create_table(&db_name, &t)
                .expect("Could not create table");
        }

        let db_meta = catalog
            .get_database_meta(&db_name)
            .expect("Could not get db meta");
        let retrieved_tbl_names: Vec<String> =
            db_meta.iter_tbl_names().map(|s| s.to_string()).collect();
        tbl_names.sort();
        assert_eq!(retrieved_tbl_names, tbl_names);
    }

    #[test]
    fn test_list_database_tables_no_entries() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        catalog
            .create_database(&db_name)
            .expect("Could not create database");
        let db_meta = catalog
            .get_database_meta(&db_name)
            .expect("Could not get db meta");
        assert_eq!(db_meta.iter_tbl_names().len(), 0);
    }
}
