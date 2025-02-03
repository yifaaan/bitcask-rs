use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::Arc;

use log::error;
use parking_lot::RwLock;

use crate::error::{Error, Result};
use crate::fio::IOManager;

pub struct FileIO {
    fd: Arc<RwLock<File>>,
}

impl FileIO {
    pub fn new(file_name: impl AsRef<Path>) -> Result<Self> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(file_name)
        {
            Ok(file) => Ok(Self {
                fd: Arc::new(RwLock::new(file)),
            }),
            Err(e) => {
                error!("open file error: {}", e);
                Err(Error::FailedToOpenDataFile)
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let file = self.fd.read();
        match file.read_at(buf, offset) {
            Ok(n) => Ok(n),
            Err(e) => {
                error!("read file error: {}", e);
                Err(Error::FailedToReadFromDataFile)
            }
        }
    }

    fn write(&self, buf: &[u8]) -> Result<usize> {
        let mut file = self.fd.write();
        match file.write(buf) {
            Ok(n) => Ok(n),
            Err(e) => {
                error!("write file error: {}", e);
                Err(Error::FailedToWriteToDataFile)
            }
        }
    }

    fn sync(&self) -> Result<()> {
        let file = self.fd.read();
        if let Err(e) = file.sync_data() {
            error!("sync file error: {}", e);
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
        let file_io = FileIO::new(&path).unwrap();
        let buf = b"Hello, world!";
        let res = file_io.write(buf).unwrap();
        assert_eq!(res, buf.len());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_file_io_read() {
        let path = PathBuf::from("/tmp/a.data");
        let file_io = FileIO::new(&path).unwrap();
        let buf = b"Hello, world!";
        let res = file_io.write(buf).unwrap();
        assert_eq!(res, buf.len());
        let mut buf = vec![0; 13];
        let res = file_io.read(&mut buf, 0).unwrap();
        assert_eq!(res, buf.len());
        assert_eq!(buf, b"Hello, world!");
        std::fs::remove_file(path).unwrap();
    }
}
