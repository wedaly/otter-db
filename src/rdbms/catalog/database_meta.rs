use crate::encode;

#[derive(Debug, PartialEq, Eq)]
pub struct DatabaseMeta {
    tbl_names: Vec<String>,
}

impl DatabaseMeta {
    pub fn new() -> DatabaseMeta {
        DatabaseMeta {
            tbl_names: Vec::new(),
        }
    }

    pub fn insert_tbl_name(&mut self, name: &str) {
        if let Err(idx) = self.tbl_names.binary_search_by(|n| n.as_str().cmp(name)) {
            self.tbl_names.insert(idx, name.to_string())
        }
    }

    pub fn iter_tbl_names(&self) -> std::slice::Iter<'_, std::string::String> {
        self.tbl_names.iter()
    }
}

impl encode::Encode for DatabaseMeta {
    fn encode(&self, w: &mut encode::BytesWriter) {
        self.tbl_names.encode(w)
    }
}

impl encode::Decode for DatabaseMeta {
    fn decode(r: &mut encode::BytesReader) -> Result<Self, encode::Error> {
        let tbl_names = Vec::<String>::decode(r)?;
        Ok(DatabaseMeta { tbl_names })
    }
}
