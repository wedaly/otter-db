use crate::encode;

#[derive(Debug, PartialEq, Eq)]
pub struct IndexMeta {}

impl IndexMeta {
    pub fn new() -> IndexMeta {
        IndexMeta {}
    }
}

impl encode::Encode for IndexMeta {
    fn encode(&self, _w: &mut encode::BytesWriter) {}
}

impl encode::Decode for IndexMeta {
    fn decode(_r: &mut encode::BytesReader) -> Result<Self, encode::Error> {
        Ok(IndexMeta {})
    }
}
