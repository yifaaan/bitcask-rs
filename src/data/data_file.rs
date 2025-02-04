use std::sync::Arc;

use crate::error::Result;
use parking_lot::RwLock;

pub struct DataFile {
    /// 文件ID
    file_id: Arc<RwLock<u32>>,
    /// 写入偏移量
    write_offset: Arc<RwLock<u64>>,
    /// IO管理器
    io_manager: Box<dyn crate::fio::IOManager>,
}

impl DataFile {
    pub fn new(dir_path: impl AsRef<Path>, file_id: u32) -> Result<Self> {
        let dir_path = dir_path.as_ref();
        let file_path = dir_path.join(format!("data_file_{}", file_id));
        let io_manager = Box::new(FileIO::new(file_path)?);
        Ok(Self {
            file_id: Arc::new(RwLock::new(file_id)),
            write_offset: Arc::new(RwLock::new(0)),
            io_manager,
        })
    }

    pub fn get_write_offset(&self) -> u64 {
        *self.write_offset.read()
    }

    pub fn get_file_id(&self) -> u32 {
        *self.file_id.read()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}
