use std::path::PathBuf;

#[derive(Clone)]
pub struct Options {
    /// 数据库目录
    pub dir_path: PathBuf,
    /// 数据文件大小
    pub data_file_size: u64,
    /// 每次写入都持久化？
    pub sync_write: bool,
    /// 索引类型
    pub index_type: IndexType,
}

#[derive(Clone)]
pub enum IndexType {
    BTree,

    SkipList,
}
