use crate::encode;

#[derive(Debug, PartialEq, Eq)]
pub struct ColumnMeta {}

impl ColumnMeta {
    pub fn new() -> ColumnMeta {
        ColumnMeta {}
    }
}

impl encode::Encode for ColumnMeta {
    fn encode(&self, _w: &mut encode::BytesWriter) {}
}

impl encode::Decode for ColumnMeta {
    fn decode(_r: &mut encode::BytesReader) -> Result<Self, encode::Error> {
        Ok(ColumnMeta {})
    }
}
