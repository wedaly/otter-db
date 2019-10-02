use crate::encode::error::Error;

pub struct BytesReader<'a> {
    cursor: usize,
    bytes: &'a [u8],
}

impl<'a> BytesReader<'a> {
    pub fn new(bytes: &'a [u8]) -> BytesReader {
        BytesReader {
            cursor: 0,
            bytes: bytes,
        }
    }

    pub fn read(&mut self, n: usize) -> Result<&[u8], Error> {
        if self.cursor + n > self.bytes.len() {
            return Err(Error::NotEnoughBytes);
        }

        let b = &self.bytes[self.cursor..self.cursor + n];
        self.cursor += n;
        Ok(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_bytes() {
        let bytes = [1, 2, 3, 4, 5];
        let mut reader = BytesReader::new(&bytes);
        assert_eq!(reader.read(2).unwrap(), &bytes[0..2]);
        assert_eq!(reader.read(3).unwrap(), &bytes[2..]);
    }

    #[test]
    fn test_not_enough_bytes() {
        let bytes = [1, 2];
        let mut reader = BytesReader::new(&bytes);
        assert_eq!(reader.read(3), Err(Error::NotEnoughBytes));
    }
}
