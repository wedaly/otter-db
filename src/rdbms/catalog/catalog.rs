use crate::kvs::Store;
use crate::rdbms::catalog::database::{Database, DatabaseNameSet};
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
            let db_key = Key::Database(db_name.to_string());
            let db_opt = self
                .store
                .get::<Database>(txn_id, KeySpace::Catalog, &db_key)?;

            if let Some(_) = db_opt {
                return Err(Error::DatabaseAlreadyExists);
            }

            self.store
                .set(txn_id, KeySpace::Catalog, &db_key, &Database::new())?;

            let mut db_set: DatabaseNameSet = self
                .store
                .get(txn_id, KeySpace::Catalog, &Key::DatabaseNameSet)?
                .unwrap_or_else(DatabaseNameSet::new);

            db_set.insert(db_name);
            self.store
                .set(txn_id, KeySpace::Catalog, &Key::DatabaseNameSet, &db_set)?;
            Ok(())
        })
    }

    pub fn get_database(&self, db_name: &str) -> Result<Database, Error> {
        self.store.with_txn(|txn_id| {
            let db_key = Key::Database(db_name.to_string());
            self.store
                .get::<Database>(txn_id, KeySpace::Catalog, &db_key)?
                .ok_or(Error::DatabaseDoesNotExist)
        })
    }

    pub fn list_databases(&self) -> Result<DatabaseNameSet, Error> {
        self.store.with_txn(|txn_id| {
            let db_set: DatabaseNameSet = self
                .store
                .get(txn_id, KeySpace::Catalog, &Key::DatabaseNameSet)?
                .unwrap_or_else(DatabaseNameSet::new);
            Ok(db_set)
        })
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
            .get_database(&db_name)
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
        let db_name_set = catalog.list_databases().expect("Could not list databases");
        let retrieved_db_names: Vec<String> = db_name_set.iter().map(|s| s.to_string()).collect();
        assert_eq!(retrieved_db_names, db_names);
    }

    #[test]
    fn test_get_database_does_not_exist() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let result = catalog.get_database(&"notexist");
        assert_eq!(result, Err(Error::DatabaseDoesNotExist));
    }

    #[test]
    fn test_list_database_no_entries() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name_set = catalog.list_databases().expect("Could not list databases");
        assert_eq!(db_name_set.iter().len(), 0);
    }
}
