mod file_io;

use crate::error::Result;

/// IO管理接口，目前支持file IO
pub trait IOManager: Sync + Send {
    /// 从文件中读取数据
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// 向文件中写入数据
    fn write(&self, buf: &[u8]) -> Result<usize>;

    /// 同步数据到磁盘
    fn sync(&self) -> Result<()>;
}
