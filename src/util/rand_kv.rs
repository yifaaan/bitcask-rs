use bytes::Bytes;

pub fn get_test_key(n: usize) -> Bytes {
    Bytes::from(format!("bitcask-rs-key-{:09}", n))
}

pub fn get_test_value(n: usize) -> Bytes {
    Bytes::from(format!("bitcask-rs-value-value-value-value-value-value-value-value-value-{:09}", n))
}

