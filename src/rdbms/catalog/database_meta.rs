use crate::encode;

#[derive(Debug, PartialEq, Eq)]
pub struct DatabaseMeta {}

impl DatabaseMeta {
    pub fn new() -> DatabaseMeta {
        DatabaseMeta {}
    }
}

impl encode::Encode for DatabaseMeta {
    fn encode(&self, _w: &mut encode::BytesWriter) {}
}

impl encode::Decode for DatabaseMeta {
    fn decode(_r: &mut encode::BytesReader) -> Result<Self, encode::Error> {
        Ok(DatabaseMeta {})
    }
}
