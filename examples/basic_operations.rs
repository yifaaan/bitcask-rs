use bitcask_rs::{db, options::Options};
use bytes::Bytes;
fn main() {
    let opts = Options::default();
    let engine = db::Engine::open(opts).expect("failed to open bitcask engine");
    let res = engine.put(Bytes::from("name"), Bytes::from("bitcask-rs"));
    assert!(res.is_ok());

    let res = engine.get(Bytes::from("name"));
    assert!(res.is_ok());
    let val = res.unwrap();
    println!("val = {:?}", String::from_utf8(val.into()));

    let res = engine.delete(Bytes::from("value"));
    assert!(res.is_ok());
}
