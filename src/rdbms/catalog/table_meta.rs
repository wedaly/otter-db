use crate::encode;

#[derive(Debug, PartialEq, Eq)]
pub struct TableMeta {}

impl TableMeta {
    pub fn new() -> TableMeta {
        TableMeta {}
    }
}

impl encode::Encode for TableMeta {
    fn encode(&self, _w: &mut encode::BytesWriter) {}
}

impl encode::Decode for TableMeta {
    fn decode(_r: &mut encode::BytesReader) -> Result<Self, encode::Error> {
        Ok(TableMeta {})
    }
}
