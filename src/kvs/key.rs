use core::hash::Hash;

pub trait Key: Hash + Eq + Clone {}

impl Key for &str {}
impl Key for &[u8] {}
impl Key for String {}
impl Key for Vec<u8> {}
impl Key for u64 {}
