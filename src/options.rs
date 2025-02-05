use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Options {
    /// 数据库目录
    pub(crate) dir_path: PathBuf,
    /// 数据文件大小
    pub(crate) data_file_size: u64,
    /// 是否持久化
    pub(crate) sync_write: bool,
    /// 索引类型
    pub(crate) index_type: IndexType,
}

/// 索引类型
#[derive(Debug, Clone, Copy)]
pub enum IndexType {
    /// BTree
    BTree,
    /// SkipList
    SkipList,
}
