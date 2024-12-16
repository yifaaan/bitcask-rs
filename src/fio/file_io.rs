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
