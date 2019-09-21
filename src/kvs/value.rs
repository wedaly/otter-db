use std::str;

pub trait ValueSink {
    fn write(&mut self, bytes: &[u8]);
}

impl ValueSink for Vec<u8> {
    fn write(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

pub trait SerializableValue {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink;
}

#[derive(Debug)]
pub enum DeserializationError {
    IncorrectLen,
    InvalidFormat(&'static str),
}

pub trait DeserializableValue
where
    Self: Sized,
{
    fn deserialize(bytes: &[u8]) -> Result<Self, DeserializationError>;
}

impl SerializableValue for &[u8] {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        sink.write(self);
    }
}

impl SerializableValue for Vec<u8> {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        sink.write(&self);
    }
}

impl DeserializableValue for Vec<u8> {
    fn deserialize(bytes: &[u8]) -> Result<Self, DeserializationError> {
        Ok(bytes.to_vec())
    }
}

impl SerializableValue for bool {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        if *self {
            sink.write(&[1]);
        } else {
            sink.write(&[0]);
        }
    }
}

impl DeserializableValue for bool {
    fn deserialize(bytes: &[u8]) -> Result<Self, DeserializationError> {
        if bytes.len() == 1 {
            Ok(bytes[0] > 0)
        } else {
            Err(DeserializationError::IncorrectLen)
        }
    }
}

impl SerializableValue for u8 {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        sink.write(&[*self])
    }
}

impl DeserializableValue for u8 {
    fn deserialize(bytes: &[u8]) -> Result<Self, DeserializationError> {
        if bytes.len() == 1 {
            Ok(bytes[0])
        } else {
            Err(DeserializationError::IncorrectLen)
        }
    }
}

impl SerializableValue for u16 {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        sink.write(&self.to_le_bytes());
    }
}

impl DeserializableValue for u16 {
    fn deserialize(bytes: &[u8]) -> Result<Self, DeserializationError> {
        if bytes.len() == 2 {
            let b: [u8; 2] = [bytes[0], bytes[1]];
            Ok(u16::from_le_bytes(b))
        } else {
            Err(DeserializationError::IncorrectLen)
        }
    }
}

impl SerializableValue for i16 {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        sink.write(&self.to_le_bytes());
    }
}

impl DeserializableValue for i16 {
    fn deserialize(bytes: &[u8]) -> Result<Self, DeserializationError> {
        if bytes.len() == 2 {
            let b: [u8; 2] = [bytes[0], bytes[1]];
            Ok(i16::from_le_bytes(b))
        } else {
            Err(DeserializationError::IncorrectLen)
        }
    }
}

impl SerializableValue for u32 {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        sink.write(&self.to_le_bytes());
    }
}

impl DeserializableValue for u32 {
    fn deserialize(bytes: &[u8]) -> Result<Self, DeserializationError> {
        if bytes.len() == 4 {
            let b: [u8; 4] = [bytes[0], bytes[1], bytes[2], bytes[3]];
            Ok(u32::from_le_bytes(b))
        } else {
            Err(DeserializationError::IncorrectLen)
        }
    }
}

impl SerializableValue for i32 {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        sink.write(&self.to_le_bytes());
    }
}

impl DeserializableValue for i32 {
    fn deserialize(bytes: &[u8]) -> Result<Self, DeserializationError> {
        if bytes.len() == 4 {
            let b: [u8; 4] = [bytes[0], bytes[1], bytes[2], bytes[3]];
            Ok(i32::from_le_bytes(b))
        } else {
            Err(DeserializationError::IncorrectLen)
        }
    }
}

impl SerializableValue for u64 {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        sink.write(&self.to_le_bytes());
    }
}

impl DeserializableValue for u64 {
    fn deserialize(bytes: &[u8]) -> Result<Self, DeserializationError> {
        if bytes.len() == 8 {
            let b: [u8; 8] = [
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ];
            Ok(u64::from_le_bytes(b))
        } else {
            Err(DeserializationError::IncorrectLen)
        }
    }
}

impl SerializableValue for i64 {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        sink.write(&self.to_le_bytes());
    }
}

impl DeserializableValue for i64 {
    fn deserialize(bytes: &[u8]) -> Result<Self, DeserializationError> {
        if bytes.len() == 8 {
            let b: [u8; 8] = [
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ];
            Ok(i64::from_le_bytes(b))
        } else {
            Err(DeserializationError::IncorrectLen)
        }
    }
}

impl SerializableValue for usize {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        sink.write(&self.to_le_bytes());
    }
}

impl DeserializableValue for usize {
    fn deserialize(bytes: &[u8]) -> Result<Self, DeserializationError> {
        if bytes.len() == 8 {
            let b: [u8; 8] = [
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ];
            Ok(usize::from_le_bytes(b))
        } else {
            Err(DeserializationError::IncorrectLen)
        }
    }
}

impl SerializableValue for &str {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        sink.write(self.as_bytes());
    }
}

impl SerializableValue for String {
    fn serialize<S>(&self, sink: &mut S)
    where
        S: ValueSink,
    {
        sink.write(self.as_bytes());
    }
}

impl DeserializableValue for String {
    fn deserialize(bytes: &[u8]) -> Result<Self, DeserializationError> {
        let s = str::from_utf8(bytes)
            .map_err(|_| DeserializationError::InvalidFormat("Invalid UTF8 string bytes"))?;
        Ok(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_byte_vec() {
        let mut sink = Vec::new();
        let input = vec![0, 5, 4, 2, 6, 255, 128, 9];
        input.serialize(&mut sink);
        let output = Vec::<u8>::deserialize(&sink).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn test_serialize_byte_slice() {
        let mut sink = Vec::new();
        let input: &[u8] = &[0, 255, 128, 3, 16];
        input.serialize(&mut sink);
        let output = Vec::<u8>::deserialize(&sink).unwrap();
        assert_eq!(input, &output[..]);
    }

    #[test]
    fn test_serialize_bool() {
        let mut sink = Vec::new();
        let (b1, b2) = (true, false);
        b1.serialize(&mut sink);
        b2.serialize(&mut sink);
        let out1 = bool::deserialize(&sink[0..1]).unwrap();
        let out2 = bool::deserialize(&sink[1..2]).unwrap();
        assert_eq!(b1, out1);
        assert_eq!(b2, out2);
    }

    #[test]
    fn test_serialize_byte() {
        let mut sink = Vec::new();
        let input = 5u8;
        input.serialize(&mut sink);
        let output = u8::deserialize(&sink).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn test_serialize_u16() {
        let mut sink = Vec::new();
        let input = 598u16;
        input.serialize(&mut sink);
        let output = u16::deserialize(&sink).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn test_serialize_i16() {
        let mut sink = Vec::new();
        let input = -598i16;
        input.serialize(&mut sink);
        let output = i16::deserialize(&sink).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn test_serialize_u32() {
        let mut sink = Vec::new();
        let input = 10456u32;
        input.serialize(&mut sink);
        let output = u32::deserialize(&sink).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn test_serialize_i32() {
        let mut sink = Vec::new();
        let input = -10456i32;
        input.serialize(&mut sink);
        let output = i32::deserialize(&sink).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn test_serialize_u64() {
        let mut sink = Vec::new();
        let input = 1041230978056u64;
        input.serialize(&mut sink);
        let output = u64::deserialize(&sink).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn test_serialize_i64() {
        let mut sink = Vec::new();
        let input = -1041230978056i64;
        input.serialize(&mut sink);
        let output = i64::deserialize(&sink).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn test_serialize_str_slice() {
        let mut sink = Vec::new();
        let input = &"xyzabcd 123456";
        input.serialize(&mut sink);
        let output = String::deserialize(&sink).unwrap();
        assert_eq!(input, &output);
    }

    #[test]
    fn test_serialize_string() {
        let mut sink = Vec::new();
        let input = "xyzabcd 123456".to_string();
        input.serialize(&mut sink);
        let output = String::deserialize(&sink).unwrap();
        assert_eq!(input, output);
    }
}
