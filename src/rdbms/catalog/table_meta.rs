use crate::encode;

#[derive(Debug, PartialEq, Eq)]
pub struct TableMeta {
    col_names: Vec<String>,
}

impl TableMeta {
    pub fn new() -> TableMeta {
        TableMeta {
            col_names: Vec::new(),
        }
    }

    pub fn insert_col_name(&mut self, name: &str) {
        if let Err(idx) = self.col_names.binary_search_by(|n| n.as_str().cmp(name)) {
            self.col_names.insert(idx, name.to_string())
        }
    }

    pub fn iter_col_names(&self) -> std::slice::Iter<'_, std::string::String> {
        self.col_names.iter()
    }
}

impl encode::Encode for TableMeta {
    fn encode(&self, w: &mut encode::BytesWriter) {
        self.col_names.encode(w)
    }
}

impl encode::Decode for TableMeta {
    fn decode(r: &mut encode::BytesReader) -> Result<Self, encode::Error> {
        let col_names = Vec::<String>::decode(r)?;
        Ok(TableMeta { col_names })
    }
}
