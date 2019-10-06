use crate::encode;

pub struct DatabaseNameSet {
    db_names: Vec<String>,
}

impl DatabaseNameSet {
    pub fn new() -> DatabaseNameSet {
        DatabaseNameSet {
            db_names: Vec::new(),
        }
    }

    pub fn insert(&mut self, name: &str) {
        if let Err(idx) = self.db_names.binary_search_by(|n| n.as_str().cmp(name)) {
            self.db_names.insert(idx, name.to_string())
        }
    }

    pub fn iter(&self) -> std::slice::Iter<'_, std::string::String> {
        self.db_names.iter()
    }
}

impl encode::Encode for DatabaseNameSet {
    fn encode(&self, w: &mut encode::BytesWriter) {
        self.db_names.encode(w);
    }
}

impl encode::Decode for DatabaseNameSet {
    fn decode(r: &mut encode::BytesReader) -> Result<Self, encode::Error> {
        let db_names = Vec::<String>::decode(r)?;
        Ok(DatabaseNameSet { db_names })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Database {}

impl Database {
    pub fn new() -> Database {
        Database {}
    }
}

impl encode::Encode for Database {
    fn encode(&self, _w: &mut encode::BytesWriter) {}
}

impl encode::Decode for Database {
    fn decode(_r: &mut encode::BytesReader) -> Result<Self, encode::Error> {
        Ok(Database {})
    }
}
