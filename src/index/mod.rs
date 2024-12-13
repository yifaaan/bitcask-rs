pub mod btree;

use crate::data::log_record::LogRecordPos;

/// 抽象索引接口，胡须如果想要接入其他的数据结构，就实现这个接口即可
pub trait Indexer {
    /// 向索引中存储key对应的数据位置信息
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    /// 根据key取出对应的索引位置信息
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    /// 根据key删除对应的索引位置信息
    fn delete(&self, key: Vec<u8>) -> bool;
}
