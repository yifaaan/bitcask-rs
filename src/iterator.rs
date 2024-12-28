use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{db::Engine, index::IndexIterator, options::IteratorOptions};

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
}

impl<'a> Iterator<'a> {
    /// 回到迭代器的起点
    fn rewind(&mut self) {
        let mut index_iter = self.index_iter.write();
        index_iter.rewind();
    }

    /// 根据传入的key，定位到到第一个大于（或小于）等于目标的key
    fn seek(&mut self, key: Vec<u8>) {
        let mut index_iter = self.index_iter.write();
        index_iter.seek(key);
    }

    /// 跳转到下一个key,value
    fn next(&mut self) -> Option<(Bytes, Bytes)> {
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
