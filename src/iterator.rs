use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{db::Engine, error::Result, index::IndexIterator, options::IteratorOptions};

/// 用户使用的迭代器
pub struct Iterator<'a> {
    /// 索引迭代器
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>,
    /// 数据库
    engine: &'a Engine,
}

impl Engine {
    /// 获取用户迭代器
    pub fn iter(&self, options: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.index.iter(options))),
            engine: self,
        }
    }

    /// 获取所有key
    pub fn list_keys(&self) -> Result<Vec<Bytes>> {
        self.index.list_keys()
    }

    /// 对所有数据执行函数操作，函数返回false就终止
    pub fn fold<F>(&self, f: F) -> Result<()>
    where
        F: Fn(Bytes, Bytes) -> bool,
    {
        let iter = self.iter(IteratorOptions::default());
        while let Some((key, value)) = iter.next() {
            if !f(key, value) {
                break;
            }
        }
        Ok(())
    }
}

impl<'a> Iterator<'a> {
    /// 回到迭代器的起点
    fn rewind(&self) {
        let mut index_iter = self.index_iter.write();
        index_iter.rewind();
    }

    /// 根据传入的key，定位到到第一个大于（或小于）等于目标的key
    fn seek(&self, key: Vec<u8>) {
        let mut index_iter = self.index_iter.write();
        index_iter.seek(key);
    }

    /// 跳转到下一个key,value
    fn next(&self) -> Option<(Bytes, Bytes)> {
        let mut index_iter = self.index_iter.write();
        if let Some(item) = index_iter.next() {
            let value = self
                .engine
                .get_value_by_position(item.1)
                .expect("failed to get value from data file");
            return Some((Bytes::from(item.0.to_vec()), value));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        options::Options,
        util::{self, rand_kv::get_test_key},
    };

    use super::*;

    #[test]
    fn test_iterator_seek() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-seek");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // 没有数据的情况
        let iter = engine.iter(IteratorOptions::default());
        iter.seek("aa".into());
        // println!("{:?}", iter.next());
        assert!(iter.next().is_none());

        // 有一条数据的情况
        let res = engine.put(Bytes::from("aacc"), get_test_key(10));
        assert!(res.is_ok());
        let iter = engine.iter(IteratorOptions::default());
        iter.seek("a".into());
        assert!(iter.next().is_some());

        // 有多条数据的情况
        let res = engine.put(Bytes::from("eecc"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("bbac"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("ccde"), get_test_key(10));
        assert!(res.is_ok());
        let iter = engine.iter(IteratorOptions::default());
        iter.seek("a".into());
        assert_eq!("aacc", iter.next().unwrap().0);
        // println!("{:?}", iter.next());

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_iterator_next() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-next");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // 有一条数据的情况
        let res = engine.put(Bytes::from("aacc"), get_test_key(10));
        assert!(res.is_ok());
        let iter = engine.iter(IteratorOptions::default());
        iter.seek("a".into());
        assert_eq!("aacc", iter.next().unwrap().0);
        assert!(iter.next().is_none());
        iter.rewind();
        assert_eq!("aacc", iter.next().unwrap().0);
        assert!(iter.next().is_none());

        // 有多条数据的情况
        let res = engine.put(Bytes::from("eecc"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("bbac"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("ccde"), get_test_key(10));
        assert!(res.is_ok());
        let mut iter_opts = IteratorOptions::default();
        iter_opts.reverse = true;
        let iter = engine.iter(iter_opts);
        while let Some(item) = iter.next() {
            // (b"aacc", b"bitcask-rs-key-000000010")
            // (b"bbac", b"bitcask-rs-key-000000010")
            // (b"ccde", b"bitcask-rs-key-000000010")
            // (b"eecc", b"bitcask-rs-key-000000010")
            // println!("{:?}", item);
            assert!(item.0.len() > 0);
        }

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_iterator_prefix() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-prefix");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let res = engine.put(Bytes::from("eecc"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("bbac"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("ccde"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("ddce"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("ddbe"), get_test_key(10));
        assert!(res.is_ok());
        let mut iter_opts = IteratorOptions::default();
        iter_opts.prefix = "dd".into();
        let iter = engine.iter(iter_opts);
        while let Some(item) = iter.next() {
            // (b"ddbe", b"bitcask-rs-key-000000010")
            // (b"ddce", b"bitcask-rs-key-000000010")
            // println!("{:?}", item);
            assert!(item.0.len() > 0);
        }

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_list_keys() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-list_keys");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let keys = engine.list_keys();
        assert!(keys.unwrap().len() == 0);

        let res = engine.put(Bytes::from("eecc"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("bbac"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("ccde"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("ddce"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("ddbe"), get_test_key(10));
        assert!(res.is_ok());
        let keys = engine.list_keys();
        // println!("{:?}", keys); Ok([b"bbac", b"ccde", b"ddbe", b"ddce", b"eecc"])
        assert!(keys.unwrap().len() == 5);

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.dir_path).expect("failed to remove path");
    }

    #[test]
    fn test_fold() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-fold");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let res = engine.put(Bytes::from("eecc"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("bbac"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("ccde"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("ddce"), get_test_key(10));
        assert!(res.is_ok());
        let res = engine.put(Bytes::from("ddbe"), get_test_key(10));
        assert!(res.is_ok());

        engine
            .fold(|key, value| {
                // key: b"bbac" value: b"bitcask-rs-key-000000010"
                // key: b"ccde" value: b"bitcask-rs-key-000000010"
                // key: b"ddbe" value: b"bitcask-rs-key-000000010"
                // key: b"ddce" value: b"bitcask-rs-key-000000010"
                // key: b"eecc" value: b"bitcask-rs-key-000000010"
                // println!("key: {:?} value: {:?}", key, value);
                assert!(key.len() > 0);
                assert!(value.len() > 0);
                true
            })
            .unwrap();
        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.dir_path).expect("failed to remove path");
    }
}
