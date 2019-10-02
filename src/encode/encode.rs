use crate::encode::error::Error;
use crate::encode::reader::BytesReader;
use crate::encode::writer::BytesWriter;
use std::str;

pub trait Encode {
    fn encode(&self, w: &mut BytesWriter);
}

pub trait Decode
where
    Self: Sized,
{
    fn decode(r: &mut BytesReader) -> Result<Self, Error>;
}

impl Encode for bool {
    fn encode(&self, w: &mut BytesWriter) {
        if *self {
            w.write(&[1]);
        } else {
            w.write(&[0]);
        }
    }
}

impl Decode for bool {
    fn decode(r: &mut BytesReader) -> Result<Self, Error> {
        let b = r.read(1)?;
        Ok(b[0] > 0)
    }
}

impl Encode for u8 {
    fn encode(&self, w: &mut BytesWriter) {
        w.write(&[*self]);
    }
}

impl Decode for u8 {
    fn decode(r: &mut BytesReader) -> Result<Self, Error> {
        let b = r.read(1)?;
        Ok(b[0])
    }
}

impl Encode for u16 {
    fn encode(&self, w: &mut BytesWriter) {
        w.write(&self.to_le_bytes());
    }
}

impl Decode for u16 {
    fn decode(r: &mut BytesReader) -> Result<Self, Error> {
        let b = r.read(2)?;
        let v: [u8; 2] = [b[0], b[1]];
        Ok(u16::from_le_bytes(v))
    }
}

impl Encode for i16 {
    fn encode(&self, w: &mut BytesWriter) {
        w.write(&self.to_le_bytes());
    }
}

impl Decode for i16 {
    fn decode(r: &mut BytesReader) -> Result<Self, Error> {
        let b = r.read(2)?;
        let v: [u8; 2] = [b[0], b[1]];
        Ok(i16::from_le_bytes(v))
    }
}

impl Encode for u32 {
    fn encode(&self, w: &mut BytesWriter) {
        w.write(&self.to_le_bytes());
    }
}

impl Decode for u32 {
    fn decode(r: &mut BytesReader) -> Result<Self, Error> {
        let b = r.read(4)?;
        let v: [u8; 4] = [b[0], b[1], b[2], b[3]];
        Ok(u32::from_le_bytes(v))
    }
}

impl Encode for i32 {
    fn encode(&self, w: &mut BytesWriter) {
        w.write(&self.to_le_bytes());
    }
}

impl Decode for i32 {
    fn decode(r: &mut BytesReader) -> Result<Self, Error> {
        let b = r.read(4)?;
        let v: [u8; 4] = [b[0], b[1], b[2], b[3]];
        Ok(i32::from_le_bytes(v))
    }
}

impl Encode for u64 {
    fn encode(&self, w: &mut BytesWriter) {
        w.write(&self.to_le_bytes());
    }
}

impl Decode for u64 {
    fn decode(r: &mut BytesReader) -> Result<Self, Error> {
        let b = r.read(8)?;
        let v: [u8; 8] = [b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]];
        Ok(u64::from_le_bytes(v))
    }
}

impl Encode for i64 {
    fn encode(&self, w: &mut BytesWriter) {
        w.write(&self.to_le_bytes());
    }
}

impl Decode for i64 {
    fn decode(r: &mut BytesReader) -> Result<Self, Error> {
        let b = r.read(8)?;
        let v: [u8; 8] = [b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]];
        Ok(i64::from_le_bytes(v))
    }
}

impl Encode for usize {
    fn encode(&self, w: &mut BytesWriter) {
        w.write(&self.to_le_bytes());
    }
}

impl Decode for usize {
    fn decode(r: &mut BytesReader) -> Result<Self, Error> {
        let b = r.read(8)?;
        let v: [u8; 8] = [b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]];
        Ok(usize::from_le_bytes(v))
    }
}

impl<V> Encode for &[V]
where
    V: Encode,
{
    fn encode(&self, w: &mut BytesWriter) {
        self.len().encode(w);
        for v in self.iter() {
            v.encode(w);
        }
    }
}

impl<V> Encode for Vec<V>
where
    V: Encode,
{
    fn encode(&self, w: &mut BytesWriter) {
        self.len().encode(w);
        for v in self.iter() {
            v.encode(w);
        }
    }
}

impl<V> Decode for Vec<V>
where
    V: Decode,
{
    fn decode(r: &mut BytesReader) -> Result<Self, Error> {
        let n = usize::decode(r)?;
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            v.push(V::decode(r)?);
        }
        Ok(v)
    }
}

impl Encode for &str {
    fn encode(&self, w: &mut BytesWriter) {
        self.as_bytes().encode(w);
    }
}

impl Encode for String {
    fn encode(&self, w: &mut BytesWriter) {
        self.as_bytes().encode(w);
    }
}

impl Decode for String {
    fn decode(w: &mut BytesReader) -> Result<Self, Error> {
        let bytes = Vec::<u8>::decode(w)?;
        let s = str::from_utf8(&bytes)
            .map_err(|_| Error::InvalidFormat("Invalid UTF8 string bytes"))?;
        Ok(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Debug;

    fn encode<V>(v: &V) -> Vec<u8>
    where
        V: Encode,
    {
        let mut buf = Vec::new();
        let mut w = BytesWriter::new(&mut buf);
        v.encode(&mut w);
        buf
    }

    fn decode<V>(bytes: &[u8]) -> V
    where
        V: Decode,
    {
        let mut reader = BytesReader::new(bytes);
        V::decode(&mut reader).unwrap()
    }

    fn check_encode_and_decode<V>(v: V)
    where
        V: Encode + Decode + Debug + Eq,
    {
        let bytes = encode(&v);
        let decoded = decode(&bytes);
        assert_eq!(v, decoded);
    }

    #[test]
    fn test_serialize_slice() {
        let slice: &[u8] = &[1, 2, 3, 4, 5];
        let bytes = encode(&slice);
        let decoded: Vec<u8> = decode(&bytes);
        assert_eq!(slice.to_vec(), decoded);
    }

    #[test]
    fn test_serialize_vec() {
        check_encode_and_decode(vec![0, 5, 4, 2, 6, 255, 128, 9]);
    }

    #[test]
    fn test_serialize_bool() {
        check_encode_and_decode(true);
        check_encode_and_decode(false);
    }

    #[test]
    fn test_serialize_byte() {
        check_encode_and_decode(5u8);
    }

    #[test]
    fn test_serialize_u16() {
        check_encode_and_decode(598u16);
    }

    #[test]
    fn test_serialize_i16() {
        check_encode_and_decode(-598i16);
    }

    #[test]
    fn test_serialize_u32() {
        check_encode_and_decode(10456u32);
    }

    #[test]
    fn test_serialize_i32() {
        check_encode_and_decode(-10456i32);
    }

    #[test]
    fn test_serialize_u64() {
        check_encode_and_decode(1041230978056u64);
    }

    #[test]
    fn test_serialize_i64() {
        check_encode_and_decode(-1041230978056i64);
    }

    #[test]
    fn test_serialize_str_ref() {
        let s = &"abcd1234";
        let bytes = encode(s);
        let decoded: String = decode(&bytes);
        assert_eq!(s.to_string(), decoded);
    }

    #[test]
    fn test_serialize_string() {
        check_encode_and_decode("xyzabcd 123456".to_string());
    }
}
