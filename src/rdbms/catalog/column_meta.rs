use crate::encode;
use crate::rdbms::DataType;

#[derive(Debug, PartialEq, Eq)]
pub struct ColumnMeta {
    data_type: DataType,
}

impl ColumnMeta {
    pub fn new(data_type: DataType) -> ColumnMeta {
        ColumnMeta { data_type }
    }
}

impl encode::Encode for ColumnMeta {
    fn encode(&self, w: &mut encode::BytesWriter) {
        self.data_type.encode(w);
    }
}

impl encode::Decode for ColumnMeta {
    fn decode(r: &mut encode::BytesReader) -> Result<Self, encode::Error> {
        let data_type = DataType::decode(r)?;
        Ok(ColumnMeta { data_type })
    }
}
