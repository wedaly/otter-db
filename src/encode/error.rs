#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    NotEnoughBytes,
    InvalidFormat(&'static str),
}
