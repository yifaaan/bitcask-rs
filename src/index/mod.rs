pub mod btree;

use crate::{
    data::log_record::LogRecordPos,
    options::{IndexType, IteratorOptions},
};

/// 抽象索引接口，胡须如果想要接入其他的数据结构，就实现这个接口即可
pub trait Indexer: Send + Sync {
    /// 向索引中存储key对应的数据位置信息
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;

    /// 根据key取出对应的索引位置信息
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// 根据key删除对应的索引位置信息
    fn delete(&self, key: Vec<u8>) -> bool;

    /// 获取索引迭代器
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexInterator>;
}

pub fn new_indexer(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => btree::BTree::new(),
        IndexType::SkipList => unimplemented!(),
    }
}

pub trait IndexInterator {
    /// 重置迭代器
    fn rewind(&mut self);

    /// 根据key，找到第一个大于（或小于）等于该key的key
    fn seek(&mut self, key: Vec<u8>);

    /// 获取下一个key
    fn next(&mut self) -> Option<(&[u8], &LogRecordPos)>;
}
