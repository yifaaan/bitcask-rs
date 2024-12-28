pub mod btree;

use bytes::Bytes;

use crate::{
    data::log_record::LogRecordPos,
    options::{IndexType, IteratorOptions},
};

/// 抽象索引接口，胡须如果想要接入其他的数据结构，就实现这个接口即可
pub trait Indexer: Sync + Send {
    /// 向索引中存储key对应的数据位置信息
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    /// 根据key取出对应的索引位置信息
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    /// 根据key删除对应的索引位置信息
    fn delete(&self, key: Vec<u8>) -> bool;
    /// 返回索引迭代器
    fn iter(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
    /// 获取所有key
    fn list_keys(&self) -> crate::error::Result<Vec<Bytes>>;
}

/// 根据类型打开内存索引
pub fn new_indexer(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => btree::BTree::new(),
        IndexType::SkipList => todo!(),
    }
}

/// 抽象索引迭代器
pub trait IndexIterator: Sync + Send {
    /// 回到迭代器的起点
    fn rewind(&mut self);

    /// 根据传入的key，定位到到第一个大于（或小于）等于目标的key
    fn seek(&mut self, key: Vec<u8>);

    /// 跳转到下一个key
    fn next(&mut self) -> Option<(&[u8], &LogRecordPos)>;
}
