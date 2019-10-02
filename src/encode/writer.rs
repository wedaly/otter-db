pub struct BytesWriter<'a> {
    buf: &'a mut Vec<u8>,
}

impl<'a> BytesWriter<'a> {
    pub fn new(buf: &'a mut Vec<u8>) -> BytesWriter {
        BytesWriter { buf }
    }

    pub fn write(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    pub fn bytes(&self) -> &[u8] {
        &self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_bytes() {
        let mut buf = Vec::new();
        let mut writer = BytesWriter::new(&mut buf);
        writer.write(&[1, 2, 3]);
        writer.write(&[4, 5]);
        assert_eq!(writer.bytes(), &[1, 2, 3, 4, 5]);
    }
}
