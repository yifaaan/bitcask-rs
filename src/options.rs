use std::path::PathBuf;

pub struct Options {
    /// 数据库目录
    pub dir_path: PathBuf,
    /// 数据文件大小
    pub data_file_size: u64,
    /// 每次写入都持久化？
    pub sync_write: bool,
}
