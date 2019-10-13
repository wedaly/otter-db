use crate::kvs::Store;
use crate::kvs::TxnId;
use crate::rdbms::catalog::column_meta::ColumnMeta;
use crate::rdbms::catalog::database_meta::DatabaseMeta;
use crate::rdbms::catalog::system_meta::SystemMeta;
use crate::rdbms::catalog::table_meta::TableMeta;
use crate::rdbms::error::Error;
use crate::rdbms::key::{Key, KeySpace};
use crate::rdbms::DataType;

pub struct Catalog<'a> {
    store: &'a Store<KeySpace, Key>,
}

impl<'a> Catalog<'a> {
    pub fn new(store: &'a Store<KeySpace, Key>) -> Catalog {
        store.define_keyspace(KeySpace::Catalog);
        Catalog { store }
    }

    pub fn create_database(&self, txn_id: TxnId, db_name: &str) -> Result<(), Error> {
        self.add_db_meta(txn_id, db_name)?;
        self.add_db_to_system_meta(txn_id, db_name)
    }

    pub fn create_table(&self, txn_id: TxnId, db_name: &str, tbl_name: &str) -> Result<(), Error> {
        self.add_tbl_meta(txn_id, db_name, tbl_name)?;
        self.add_tbl_to_db_meta(txn_id, db_name, tbl_name)
    }

    pub fn create_column(
        &self,
        txn_id: TxnId,
        db_name: &str,
        tbl_name: &str,
        col_name: &str,
        data_type: DataType,
    ) -> Result<(), Error> {
        self.add_col_meta(txn_id, db_name, tbl_name, col_name, data_type)?;
        self.add_col_to_tbl_meta(txn_id, db_name, tbl_name, col_name)
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

    pub fn get_column_meta(
        &self,
        txn_id: TxnId,
        db_name: &str,
        tbl_name: &str,
        col_name: &str,
    ) -> Result<ColumnMeta, Error> {
        let col_meta_key = Key::ColumnMeta {
            db: db_name.to_string(),
            tbl: tbl_name.to_string(),
            col: col_name.to_string(),
        };
        self.store
            .get::<ColumnMeta>(txn_id, KeySpace::Catalog, &col_meta_key)?
            .ok_or(Error::ColumnDoesNotExist)
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

    fn add_col_meta(
        &self,
        txn_id: TxnId,
        db_name: &str,
        tbl_name: &str,
        col_name: &str,
        data_type: DataType,
    ) -> Result<(), Error> {
        let col_meta_key = Key::ColumnMeta {
            db: db_name.to_string(),
            tbl: tbl_name.to_string(),
            col: col_name.to_string(),
        };

        let col_meta_opt =
            self.store
                .get::<ColumnMeta>(txn_id, KeySpace::Catalog, &col_meta_key)?;

        if let Some(_) = col_meta_opt {
            return Err(Error::ColumnAlreadyExists);
        }

        self.store
            .set(
                txn_id,
                KeySpace::Catalog,
                &col_meta_key,
                &ColumnMeta::new(data_type),
            )
            .map_err(From::from)
    }

    fn add_col_to_tbl_meta(
        &self,
        txn_id: TxnId,
        db_name: &str,
        tbl_name: &str,
        col_name: &str,
    ) -> Result<(), Error> {
        let tbl_meta_key = Key::TableMeta {
            db: db_name.to_string(),
            tbl: tbl_name.to_string(),
        };

        let mut tbl_meta = self
            .store
            .get::<TableMeta>(txn_id, KeySpace::Catalog, &tbl_meta_key)?
            .ok_or(Error::TableDoesNotExist)?;

        tbl_meta.insert_col_name(col_name);

        self.store
            .set(txn_id, KeySpace::Catalog, &tbl_meta_key, &tbl_meta)
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

    #[test]
    fn test_create_column() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let tbl_name = "testtbl";
        let col_name = "testcol";
        let result: Result<(), Error> = store.with_txn(|txn_id| {
            catalog.create_database(txn_id, &db_name)?;
            catalog.create_table(txn_id, &db_name, &tbl_name)?;
            catalog.create_column(txn_id, &db_name, &tbl_name, &col_name, DataType::Int64)
        });
        assert_eq!(result.is_ok(), true, "Error occurred {:?}", result.err());
    }

    #[test]
    fn test_create_column_tbl_does_not_exist() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let tbl_name = "testtbl";
        let col_name = "testcol";
        let result: Result<(), Error> = store.with_txn(|txn_id| {
            catalog.create_column(txn_id, &db_name, &tbl_name, &col_name, DataType::Int64)
        });
        assert_eq!(result, Err(Error::TableDoesNotExist));
    }

    #[test]
    fn test_create_column_already_exists() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let tbl_name = "testtbl";
        let col_name = "testcol";
        let result: Result<(), Error> = store.with_txn(|txn_id| {
            catalog.create_database(txn_id, &db_name)?;
            catalog.create_table(txn_id, &db_name, &tbl_name)?;
            catalog.create_column(txn_id, &db_name, &tbl_name, &col_name, DataType::Int64)?;
            catalog.create_column(txn_id, &db_name, &tbl_name, &col_name, DataType::Int64)
        });
        assert_eq!(result, Err(Error::ColumnAlreadyExists));
    }

    #[test]
    fn test_list_columns() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let tbl_name = "testtbl";
        let mut col_names = vec!["foo", "bar", "baz"];
        let result: Result<Vec<String>, Error> = store.with_txn(|txn_id| {
            catalog.create_database(txn_id, &db_name)?;
            catalog.create_table(txn_id, &db_name, &tbl_name)?;
            for c in col_names.iter() {
                catalog.create_column(txn_id, &db_name, &tbl_name, &c, DataType::Int64)?;
            }
            let tbl_meta = catalog.get_table_meta(txn_id, &db_name, &tbl_name)?;
            let retrieved_col_names: Vec<String> =
                tbl_meta.iter_col_names().map(|s| s.to_string()).collect();
            Ok(retrieved_col_names)
        });
        let retrieved_col_names = result.expect("Could not retrieve column names");
        col_names.sort();
        assert_eq!(retrieved_col_names, col_names);
    }

    #[test]
    fn test_list_columns_no_entries() {
        let store = Store::new();
        let catalog = Catalog::new(&store);
        let db_name = "testdb";
        let tbl_name = "testtbl";
        let result: Result<(), Error> = store.with_txn(|txn_id| {
            catalog.create_database(txn_id, &db_name)?;
            catalog.create_table(txn_id, &db_name, &tbl_name)?;
            let tbl_meta = catalog.get_table_meta(txn_id, &db_name, &tbl_name)?;
            assert_eq!(tbl_meta.iter_col_names().len(), 0);
            Ok(())
        });
        assert_eq!(result.is_ok(), true, "Error occurred: {:?}", result.err());
    }
}
