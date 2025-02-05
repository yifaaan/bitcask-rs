use bitcask_rs::{db, options::Options};
use bytes::Bytes;

fn main() {
    let opts = Options::default();
    let engine = db::Engine::open(opts).expect("failed to open database");

    let key = Bytes::from("hello");
    let value = Bytes::from("bitcask-rs");
    engine.put(key, value).expect("failed to put");

    let value = engine.get(Bytes::from("hello")).expect("failed to get");
    println!("value: {:?}", value);

    // engine.delete(Bytes::from("hello")).expect("failed to delete");
}
