use crate::encode;

pub struct SystemMeta {
    db_names: Vec<String>,
}

impl SystemMeta {
    pub fn new() -> SystemMeta {
        SystemMeta {
            db_names: Vec::new(),
        }
    }

    pub fn insert_db_name(&mut self, name: &str) {
        if let Err(idx) = self.db_names.binary_search_by(|n| n.as_str().cmp(name)) {
            self.db_names.insert(idx, name.to_string())
        }
    }

    pub fn iter_db_names(&self) -> std::slice::Iter<'_, std::string::String> {
        self.db_names.iter()
    }
}

impl encode::Encode for SystemMeta {
    fn encode(&self, w: &mut encode::BytesWriter) {
        self.db_names.encode(w);
    }
}

impl encode::Decode for SystemMeta {
    fn decode(r: &mut encode::BytesReader) -> Result<Self, encode::Error> {
        let db_names = Vec::<String>::decode(r)?;
        Ok(SystemMeta { db_names })
    }
}
