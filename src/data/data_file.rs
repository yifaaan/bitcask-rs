use std::path::Path;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::Result;
use crate::fio;

/// 数据文件
pub struct DataFile {
    /// 数据文件id
    file_id: Arc<RwLock<u32>>,
    /// 数据文件的当前写偏移
    write_offset: Arc<RwLock<u64>>,
    /// IO管理接口
    io_manager: Box<dyn fio::IOManager>,
}

impl DataFile {
    pub fn new(dir_path: impl AsRef<Path>, file_id: u32) -> Result<Self> {
        todo!()
    }
    /// 获得数据文件的当前写偏移
    pub fn get_write_offset(&self) -> u64 {
        *self.write_offset.read()
    }
    /// 获得数据文件id
    pub fn get_file_id(&self) -> u32 {
        *self.file_id.read()
    }
    /// 写入数据文件
    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        self.io_manager.write(buf)
    }
    /// 持久化数据文件
    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }
}