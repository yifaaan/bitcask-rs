pub mod file_io;

use crate::error::Result;

/// 抽象IO管理接口，可以接入不同的IO类型，目前支持标准文件IO
pub trait IOManager: Sync + Send {
    /// 从文件给定位置读取数据
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    /// 写入字节数组到文件中
    fn write(&self, buf: &[u8]) -> Result<usize>;
    /// 持久化
    fn sync(&self) -> Result<()>;
}

pub fn new_io_manager(file_name: impl AsRef<std::path::Path>) -> Result<impl IOManager> {
    file_io::FileIO::new(file_name)
}
