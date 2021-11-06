macro_rules! new_kv {
    ($k:expr, $v:expr) => {
        crate::mrapps::wc::worker::KeyValue { key: $k, val: $v }
    };
}

pub struct KeyValue<T, U> {
    pub key: T,
    pub val: U,
}
