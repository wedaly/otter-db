use crate::encode;

#[derive(Debug, Eq, PartialEq)]
pub enum DataType {
    Int64,
}

const INT64_CODE: u8 = 0;

impl encode::Encode for DataType {
    fn encode(&self, w: &mut encode::BytesWriter) {
        let code = match self {
            DataType::Int64 => INT64_CODE,
        };
        code.encode(w)
    }
}

impl encode::Decode for DataType {
    fn decode(r: &mut encode::BytesReader) -> Result<Self, encode::Error> {
        let code = u8::decode(r)?;
        match code {
            INT64_CODE => Ok(DataType::Int64),
            _ => Err(encode::Error::InvalidFormat("Unrecognized datatype")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encode::{Decode, Encode};

    fn check_encode_and_decode(input: DataType) {
        let mut buf = Vec::new();
        let mut w = encode::BytesWriter::new(&mut buf);
        input.encode(&mut w);
        let mut r = encode::BytesReader::new(&buf);
        let output = DataType::decode(&mut r).expect("Could not decode");
        assert_eq!(input, output);
    }

    #[test]
    fn it_encodes_int64_type() {
        check_encode_and_decode(DataType::Int64);
    }
}
