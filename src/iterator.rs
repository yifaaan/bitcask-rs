use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    data::log_record::LogRecordPos, db::Engine, index::IndexInterator, options::IteratorOptions,
};

pub struct Iterator<'a> {
    index_iter: Arc<RwLock<Box<dyn IndexInterator>>>,
    engine: &'a Engine,
}

impl Engine {
    pub fn iter(&self, options: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.index.iterator(options))),
            engine: self,
        }
    }
}

impl<'a> Iterator<'a> {
    /// 重置迭代器
    pub fn rewind(&self) {
        self.index_iter.write().rewind();
    }

    /// 根据key，找到第一个大于（或小于）等于该key的key
    pub fn seek(&self, key: Vec<u8>) {
        self.index_iter.write().seek(key);
    }

    /// 获取下一个(key, value)
    pub fn next(&self) -> Option<(Bytes, Bytes)> {
        let mut index_iter = self.index_iter.write();
        match index_iter.next() {
            Some((key, pos)) => {
                let value = self.engine.get_value_by_position(&pos).expect(&format!(
                    "failed to get value by position, key is {:?}, pos is {:?}",
                    key, pos
                ));
                Some((key.to_vec().into(), value))
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        options::Options,
        util::rand_kv::{get_test_key, get_test_value},
    };

    use super::*;

    #[test]
    fn test_iterator_seek() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iterator-seek");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // no data
        let iter = engine.iter(IteratorOptions::default());
        iter.seek(get_test_key(11).to_vec());
        assert!(iter.next().is_none());

        // 正常数据
        let put_res = engine.put(get_test_key(11), get_test_value(11));
        assert!(put_res.is_ok());
        let iter = engine.iter(IteratorOptions::default());
        iter.seek(get_test_key(11).to_vec());
        assert!(iter.next().is_some());

        let put_res = engine.put("aaabcd".into(), "value1".into());
        let put_res = engine.put("ababcd".into(), "value2".into());
        let put_res = engine.put("acabcd".into(), "value3".into());
        let put_res = engine.put("baabcd".into(), "value4".into());
        let put_res = engine.put("bbabcd".into(), "value5".into());
        let iter = engine.iter(IteratorOptions::default());
        iter.seek("ac".into());
        assert_eq!(iter.next().unwrap().1, "value3");
        assert_eq!(iter.next().unwrap().1, "value4");
        assert_eq!(iter.next().unwrap().1, "value5");

        let iter = engine.iter(IteratorOptions::default());
        iter.seek("z".into());
        assert!(iter.next().is_none());
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_iterator_next() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iterator-next");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let put_res = engine.put("aaabcd".into(), "value1".into());
        let put_res = engine.put("ababcd".into(), "value2".into());
        let put_res = engine.put("acabcd".into(), "value3".into());
        let put_res = engine.put("baabcd".into(), "value4".into());
        let put_res = engine.put("bbabcd".into(), "value5".into());
        let iter = engine.iter(IteratorOptions::default());
        assert_eq!(iter.next().unwrap().1, "value1");
        assert_eq!(iter.next().unwrap().1, "value2");
        assert_eq!(iter.next().unwrap().1, "value3");
        assert_eq!(iter.next().unwrap().1, "value4");
        assert_eq!(iter.next().unwrap().1, "value5");
        assert!(iter.next().is_none());
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_iterator_rewind() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iterator-rewind");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let put_res = engine.put("aaabcd".into(), "value1".into());
        let put_res = engine.put("ababcd".into(), "value2".into());
        let put_res = engine.put("acabcd".into(), "value3".into());
        let put_res = engine.put("baabcd".into(), "value4".into());
        let put_res = engine.put("bbabcd".into(), "value5".into());
        let iter = engine.iter(IteratorOptions::default());
        assert_eq!(iter.next().unwrap().1, "value1");
        assert_eq!(iter.next().unwrap().1, "value2");
        assert_eq!(iter.next().unwrap().1, "value3");
        assert_eq!(iter.next().unwrap().1, "value4");
        assert_eq!(iter.next().unwrap().1, "value5");
        assert!(iter.next().is_none());

        iter.rewind();
        assert_eq!(iter.next().unwrap().1, "value1");
        assert_eq!(iter.next().unwrap().1, "value2");
        assert_eq!(iter.next().unwrap().1, "value3");
        assert_eq!(iter.next().unwrap().1, "value4");
        assert_eq!(iter.next().unwrap().1, "value5");
        assert!(iter.next().is_none());
        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_iterator_prefix() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-iterator-prefix");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let put_res = engine.put("aaabcd".into(), "value1".into());
        let put_res = engine.put("ababcd".into(), "value2".into());
        let put_res = engine.put("acabcd".into(), "value3".into());
        let put_res = engine.put("baabcd".into(), "value4".into());
        let put_res = engine.put("bbabcd".into(), "value5".into());
        let put_res = engine.put("abbbcd".into(), "value6".into());
        let mut iter_opts = IteratorOptions::default();
        iter_opts.prefix = "ab".into();
        let iter = engine.iter(iter_opts);
        assert_eq!(iter.next().unwrap().1, "value2");
        assert_eq!(iter.next().unwrap().1, "value6");
        assert!(iter.next().is_none());

        let mut iter_opts = IteratorOptions::default();
        iter_opts.prefix = "b".into();
        let iter = engine.iter(iter_opts);
        assert_eq!(iter.next().unwrap().1, "value4");
        assert_eq!(iter.next().unwrap().1, "value5");
        assert!(iter.next().is_none());

        std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove dir");
    }
}
