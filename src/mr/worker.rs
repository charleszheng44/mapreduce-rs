#[derive(Debug)]
pub struct KeyValue<T, U> {
    pub key: T,
    pub val: U,
}

impl<T, U> KeyValue<T, U> {
    pub fn new(key: T, val: U) -> Self {
        KeyValue { key, val }
    }
}
