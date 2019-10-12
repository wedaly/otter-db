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

    pub fn create_database(&self, txn_id: TxnId, db_name: &str) -> Result<(), Error> {
        self.add_db_meta(txn_id, db_name)
            .and_then(|_| self.add_db_to_system_meta(txn_id, db_name))
    }

    pub fn create_table(&self, txn_id: TxnId, db_name: &str, tbl_name: &str) -> Result<(), Error> {
        self.add_tbl_meta(txn_id, db_name, tbl_name)
            .and_then(|_| self.add_tbl_to_db_meta(txn_id, db_name, tbl_name))
    }

    pub fn get_system_meta(&self, txn_id: TxnId) -> Result<SystemMeta, Error> {
        self.get_or_create_system_meta(txn_id)
    }

    pub fn get_database_meta(&self, txn_id: TxnId, db_name: &str) -> Result<DatabaseMeta, Error> {
        let db_meta_key = Key::DatabaseMeta {
            db: db_name.to_string(),
        };
        self.store
            .get::<DatabaseMeta>(txn_id, KeySpace::Catalog, &db_meta_key)?
            .ok_or(Error::DatabaseDoesNotExist)
    }

    pub fn get_table_meta(
        &self,
        txn_id: TxnId,
        db_name: &str,
        tbl_name: &str,
    ) -> Result<TableMeta, Error> {
        let tbl_meta_key = Key::TableMeta {
            db: db_name.to_string(),
            tbl: tbl_name.to_string(),
        };
        self.store
            .get::<TableMeta>(txn_id, KeySpace::Catalog, &tbl_meta_key)?
            .ok_or(Error::TableDoesNotExist)
    }

    fn get_or_create_system_meta(&self, txn_id: TxnId) -> Result<SystemMeta, Error> {
        let system_meta = self
            .store
            .get(txn_id, KeySpace::Catalog, &Key::SystemMeta)?
            .unwrap_or_else(SystemMeta::new);
        Ok(system_meta)
    }

    fn add_db_meta(&self, txn_id: TxnId, db_name: &str) -> Result<(), Error> {
        let db_meta_key = Key::DatabaseMeta {
            db: db_name.to_string(),
        };
        let db_meta_opt =
            self.store
                .get::<DatabaseMeta>(txn_id, KeySpace::Catalog, &db_meta_key)?;

        if let Some(_) = db_meta_opt {
            return Err(Error::DatabaseAlreadyExists);
        }

        self.store
            .set(
                txn_id,
                KeySpace::Catalog,
                &db_meta_key,
                &DatabaseMeta::new(),
            )
            .map_err(From::from)
    }

    fn add_db_to_system_meta(&self, txn_id: TxnId, db_name: &str) -> Result<(), Error> {
        let mut system_meta: SystemMeta = self.get_or_create_system_meta(txn_id)?;

        system_meta.insert_db_name(db_name);

        self.store
            .set(txn_id, KeySpace::Catalog, &Key::SystemMeta, &system_meta)
            .map_err(From::from)
    }

    fn add_tbl_meta(&self, txn_id: TxnId, db_name: &str, tbl_name: &str) -> Result<(), Error> {
        let tbl_meta_key = Key::TableMeta {
            db: db_name.to_string(),
            tbl: tbl_name.to_string(),
        };

        let tbl_meta_opt = self
            .store
            .get::<TableMeta>(txn_id, KeySpace::Catalog, &tbl_meta_key)?;

        if let Some(_) = tbl_meta_opt {
            return Err(Error::TableAlreadyExists);
        }

        self.store
            .set(txn_id, KeySpace::Catalog, &tbl_meta_key, &TableMeta::new())
            .map_err(From::from)
    }

    fn add_tbl_to_db_meta(
        &self,
        txn_id: TxnId,
        db_name: &str,
        tbl_name: &str,
    ) -> Result<(), Error> {
        let db_meta_key = Key::DatabaseMeta {
            db: db_name.to_string(),
        };

        let mut db_meta = self
            .store
            .get::<DatabaseMeta>(txn_id, KeySpace::Catalog, &db_meta_key)?
            .ok_or(Error::DatabaseDoesNotExist)?;

        db_meta.insert_tbl_name(tbl_name);

        self.store
            .set(txn_id, KeySpace::Catalog, &db_meta_key, &db_meta)
            .map_err(From::from)
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
        let result: Result<DatabaseMeta, Error> = store.with_txn(|txn_id| {
            catalog.create_database(txn_id, &db_name)?;
            catalog.get_database_meta(txn_id, &db_name)
        });
        assert_eq!(result.is_ok(), true, "Error occurred: {:?}", result.err());
    }

    #[test]
    fn test_create_database_already_exists() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let result: Result<(), Error> = store.with_txn(|txn_id| {
            catalog.create_database(txn_id, &db_name)?;
            catalog.create_database(txn_id, &db_name)?;
            Ok(())
        });
        assert_eq!(result, Err(Error::DatabaseAlreadyExists));
    }

    #[test]
    fn test_create_and_list_databases() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_names = vec!["testdb1", "testdb2", "testdb3"];
        let result: Result<Vec<String>, Error> = store.with_txn(|txn_id| {
            for db_name in db_names.iter() {
                catalog.create_database(txn_id, &db_name)?;
            }
            let system_meta: SystemMeta = catalog.get_system_meta(txn_id)?;
            let retrieved_db_names: Vec<String> =
                system_meta.iter_db_names().map(|s| s.to_string()).collect();
            Ok(retrieved_db_names)
        });
        let retrieved_db_names = result.expect("Could not retrieve db names");
        assert_eq!(retrieved_db_names, db_names);
    }

    #[test]
    fn test_get_database_does_not_exist() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let result = store.with_txn(|txn_id| catalog.get_database_meta(txn_id, &"notexist"));
        assert_eq!(result, Err(Error::DatabaseDoesNotExist));
    }

    #[test]
    fn test_list_database_no_entries() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let result: Result<(), Error> = store.with_txn(|txn_id| {
            let system_meta = catalog.get_system_meta(txn_id)?;
            assert_eq!(system_meta.iter_db_names().len(), 0);
            Ok(())
        });
        assert_eq!(result.is_ok(), true, "Error occurred: {:?}", result.err());
    }

    #[test]
    fn test_create_table() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let tbl_names = vec!["foo", "bar", "baz"];
        let result: Result<(), Error> = store.with_txn(|txn_id| {
            catalog.create_database(txn_id, &db_name)?;

            for t in tbl_names.iter() {
                catalog.create_table(txn_id, &db_name, &t)?;
            }

            for t in tbl_names.iter() {
                catalog.get_table_meta(txn_id, db_name, t)?;
            }

            Ok(())
        });
        assert_eq!(result.is_ok(), true, "Error occurred: {:?}", result.err());
    }

    #[test]
    fn test_create_table_database_does_not_exist() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let result: Result<(), Error> =
            store.with_txn(|txn_id| catalog.create_table(txn_id, &"notexists", &"foo"));
        assert_eq!(result, Err(Error::DatabaseDoesNotExist));
    }

    #[test]
    fn test_create_table_already_exists() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let tbl_name = "testtbl";
        let result: Result<(), Error> = store.with_txn(|txn_id| {
            catalog.create_database(txn_id, &db_name)?;
            catalog.create_table(txn_id, &db_name, &tbl_name)?;
            catalog.create_table(txn_id, &db_name, &tbl_name)
        });
        assert_eq!(result, Err(Error::TableAlreadyExists));
    }

    #[test]
    fn test_list_database_tables() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let mut tbl_names = vec!["foo", "bar", "baz"];
        let result: Result<Vec<String>, Error> = store.with_txn(|txn_id| {
            catalog.create_database(txn_id, &db_name)?;

            for t in tbl_names.iter() {
                catalog.create_table(txn_id, &db_name, &t)?;
            }

            let db_meta = catalog.get_database_meta(txn_id, &db_name)?;
            let tbl_names: Vec<String> = db_meta.iter_tbl_names().map(|s| s.to_string()).collect();
            Ok(tbl_names)
        });

        let retrieved_tbl_names = result.expect("Could not retrieve table names");
        tbl_names.sort();
        assert_eq!(retrieved_tbl_names, tbl_names);
    }

    #[test]
    fn test_list_database_tables_no_entries() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let result: Result<DatabaseMeta, Error> = store.with_txn(|txn_id| {
            catalog.create_database(txn_id, &db_name)?;
            catalog.get_database_meta(txn_id, &db_name)
        });
        let db_meta = result.expect("Could not retrieve db meta");
        assert_eq!(db_meta.iter_tbl_names().len(), 0);
    }
}
