use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::FileExt,
    path::Path,
    sync::Arc,
};

use parking_lot::RwLock;

use super::IOManager;
use crate::error::{Error, Result};

/// 标准系统文件IO
pub struct FileIO {
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new(file_name: impl AsRef<Path>) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(file_name.as_ref())
        {
            Ok(file) => Ok(Self {
                fd: Arc::new(RwLock::new(file)),
            }),
            Err(e) => {
                log::error!("Failed to open data file: {}", e);
                Err(Error::FailedToOpenDataFile)
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let read_guard = self.fd.read();
        match read_guard.read_at(buf, offset) {
            Ok(n) => Ok(n),
            Err(e) => {
                log::error!("Read from data file: {}", e);
                Err(Error::FailedToReadFromDataFile)
            }
        }
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut write_guard = self.fd.write();
        match write_guard.write(buf) {
            Ok(n) => Ok(n),
            Err(e) => {
                log::error!("Write to data file: {}", e);
                Err(Error::FailedToWriteToDataFile)
            }
        }
    }

    fn sync(&self) -> Result<()> {
        let read_guard = self.fd.read();
        if let Err(e) = read_guard.sync_all() {
            log::error!("Failed to sync data file: {}", e);
            Err(Error::FailedToSyncDataFile)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_file_io_write() {
        let path = PathBuf::from("/tmp/a.data");
        let file = FileIO::new(&path).unwrap();

        let res = file.write("key-a".as_bytes()).unwrap();
        assert_eq!(res, 5);

        let res = file.write("key-b".as_bytes()).unwrap();
        assert_eq!(res, 5);

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_file_io_read() {
        let path = PathBuf::from("/tmp/b.data");
        let file = FileIO::new(&path).unwrap();

        let res = file.write("key-a".as_bytes()).unwrap();
        assert_eq!(res, 5);

        let res = file.write("key-b".as_bytes()).unwrap();
        assert_eq!(res, 5);

        let mut buf = [0; 5];
        file.read(&mut buf, 0).unwrap();
        assert_eq!(buf, "key-a".as_bytes());

        file.read(&mut buf, 5).unwrap();
        assert_eq!(buf, "key-b".as_bytes());

        std::fs::remove_file(path).unwrap();
    }
}
