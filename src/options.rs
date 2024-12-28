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

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask-rs"),
            data_file_size: 256 * 1024 * 1024, // 256MB
            sync_write: false,
            index_type: IndexType::BTree,
        }
    }
}

#[derive(Clone)]
pub enum IndexType {
    BTree,

    SkipList,
}

/// 索引迭代器配置
#[derive(Clone)]
pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}
