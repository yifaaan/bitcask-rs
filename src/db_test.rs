use std::path::PathBuf;

use bytes::Bytes;

use crate::{
    db::Engine,
    error::Error,
    options::Options,
    util::rand_kv::{get_test_key, get_test_value},
};

#[test]
fn test_engine_put() {
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitcask-rs-put");
    opts.data_file_size = 64 * 1024 * 1024;
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    // 1.正常Put一条数据
    let res = engine.put(get_test_key(11), get_test_value(11));
    assert!(res.is_ok());
    let res = engine.get(get_test_key(11));
    assert!(res.is_ok());
    assert!(res.unwrap().len() > 0);

    // 2.重复Put key相同的数据
    let res = engine.put(get_test_key(22), get_test_value(22));
    assert!(res.is_ok());
    let res = engine.put(get_test_key(22), Bytes::from("a new value"));
    assert!(res.is_ok());
    let res = engine.get(get_test_key(22));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), Bytes::from("a new value"));

    // 3.key为空
    let res = engine.put(Bytes::new(), get_test_value(123));
    assert_eq!(Error::KeyIsEmpty, res.err().unwrap());

    // 4.value为空
    let res = engine.put(get_test_key(33), Bytes::new());
    assert!(res.is_ok());
    let res = engine.get(get_test_key(33));
    assert_eq!(res.unwrap().len(), 0);

    // 5.写到数据文件进行了转换
    for i in 0..=1000000 {
        let res = engine.put(get_test_key(i), get_test_value(i));
        assert!(res.is_ok());
    }

    // 6.重启后再Put数据
    std::mem::drop(engine);

    let engine = Engine::open(opts.clone()).expect("failed to open engine");
    let res = engine.put(get_test_key(55), get_test_value(55));
    assert!(res.is_ok());

    let res = engine.get(get_test_key(55));
    assert_eq!(res.unwrap(), get_test_value(55));

    // 删除测试的文件夹
    std::fs::remove_dir_all(opts.dir_path).expect("failed to remove path");
}

#[test]
fn test_engine_get() {
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitcask-rs-get");
    opts.data_file_size = 64 * 1024 * 1024;
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    // 1.正常读取一条数据
    let res = engine.put(get_test_key(111), get_test_value(111));
    assert!(res.is_ok());
    let res: Result<Bytes, Error> = engine.get(get_test_key(111));
    assert!(res.is_ok());
    assert!(res.unwrap().len() > 0);

    // 2.读取一个不存在的key
    let res = engine.get(Bytes::from("not existed key"));
    assert_eq!(Error::KeyNotFound, res.err().unwrap());

    // 3. 值被重复Put后再读取
    let res = engine.put(get_test_key(222), get_test_value(222));
    assert!(res.is_ok());
    let res = engine.put(get_test_key(222), Bytes::from("a new value"));
    assert!(res.is_ok());
    let res = engine.get(get_test_key(222));
    assert_eq!(res.unwrap(), Bytes::from("a new value"));

    // 4.值被删除后再Get
    let res = engine.put(get_test_key(333), get_test_value(333));
    assert!(res.is_ok());
    let res = engine.delete(get_test_key(333));
    assert!(res.is_ok());
    let res = engine.get(get_test_key(333));
    assert_eq!(Error::KeyNotFound, res.err().unwrap());

    // 5.转换成了旧的数据文件，从旧的数据文件获取value
    for i in 500..=1000000 {
        let res = engine.put(get_test_key(i), get_test_value(i));
        assert!(res.is_ok());
    }
    let res = engine.get(get_test_key(505));
    assert_eq!(get_test_value(505), res.unwrap());

    // 6.重启后，前面写入的数据都能拿到
    std::mem::drop(engine);

    let engine = Engine::open(opts.clone()).expect("failed to open engine");
    let res = engine.get(get_test_key(111));
    assert_eq!(get_test_value(111), res.unwrap());
    let res = engine.get(get_test_key(222));
    assert_eq!(Bytes::from("a new value"), res.unwrap());
    let res = engine.get(get_test_key(333));
    assert_eq!(Error::KeyNotFound, res.err().unwrap());

    // 删除测试的文件夹
    std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
}

#[test]
fn test_engine_delete() {
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitcask-rs-get");
    opts.data_file_size = 64 * 1024 * 1024;
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    // 1.正常删除一个存在的key
    let res = engine.put(get_test_key(111), get_test_value(111));
    assert!(res.is_ok());
    let res = engine.delete(get_test_key(111));
    assert!(res.is_ok());
    let res = engine.get(get_test_key(111));
    assert_eq!(Error::KeyNotFound, res.err().unwrap());

    // 2.删除一个不存在的key
    let res = engine.delete(Bytes::from("not-existed-key"));
    assert!(res.is_ok());

    // 3.删除一个空的key
    let res = engine.delete(Bytes::new());
    assert_eq!(Error::KeyIsEmpty, res.err().unwrap());

    // 4.值被删除后重新Put
    let res = engine.put(get_test_key(222), get_test_value(222));
    assert!(res.is_ok());
    let res = engine.delete(get_test_key(222));
    assert!(res.is_ok());
    let res = engine.put(get_test_key(222), Bytes::from("a new value"));
    assert!(res.is_ok());
    let res = engine.get(get_test_key(222));
    assert_eq!(Bytes::from("a new value"), res.unwrap());

    // 5.重启后再Put数据
    std::mem::drop(engine);

    let engine = Engine::open(opts.clone()).expect("failed to open engine");
    let res = engine.get(get_test_key(111));
    assert_eq!(Error::KeyNotFound, res.err().unwrap());
    let res = engine.get(get_test_key(222));
    assert_eq!(Bytes::from("a new value"), res.unwrap());

    // 删除测试的文件夹
    std::fs::remove_dir_all(opts.dir_path).expect("failed to remove path");
}
