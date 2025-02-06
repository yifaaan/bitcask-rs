use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    data::log_record::max_log_record_header_size,
    error::{Error, Result},
    fio::new_io_manager,
};
use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};

use super::log_record::{LogRecord, ReadLogRecord};

pub const DATA_FILE_SUFFIX: &str = ".data";

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
        let file_path = get_data_file_full_path(&dir_path, file_id);
        let io_manager = new_io_manager(file_path)?;
        Ok(Self {
            file_id: Arc::new(RwLock::new(file_id)),
            write_offset: Arc::new(RwLock::new(0)),
            io_manager: Box::new(io_manager),
        })
    }

    pub fn get_write_offset(&self) -> u64 {
        *self.write_offset.read()
    }

    pub fn get_file_id(&self) -> u32 {
        *self.file_id.read()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let n_bytes = self.io_manager.write(buf)?;
        // 更新写入偏移量
        *self.write_offset.write() += n_bytes as u64;
        Ok(n_bytes)
    }

    /// 从offset处读取log record
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        // log record 的结构
        // 1 byte for log record type
        // var bytes for key length
        // var bytes for value length
        // key
        // value
        // 4 bytes for crc

        // 读取header
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());
        self.io_manager.read(header_buf.as_mut(), offset)?;
        // 解析header, 获取record type, key length, value length
        let record_type = header_buf.get_u8();
        let key_len = decode_length_delimiter(&mut header_buf).unwrap();
        let value_len = decode_length_delimiter(&mut header_buf).unwrap();
        // 如果key length和value length都为0, 则表示文件结束
        if key_len == 0 && value_len == 0 {
            return Err(Error::ReadDataFileEOF);
        }
        // 计算实际的header大小(编码后)
        let actual_header_size =
            length_delimiter_len(key_len) + length_delimiter_len(value_len) + 1;
        // 读取key, value
        let mut kv_buf = BytesMut::zeroed(key_len + value_len + 4);
        self.io_manager
            .read(&mut kv_buf, offset + actual_header_size as u64)?;
        // 构造log record
        let log_record = LogRecord {
            key: kv_buf.get(..key_len).unwrap().into(),
            value: kv_buf.get(key_len..kv_buf.len() - 4).unwrap().into(),
            record_type: record_type.into(),
        };
        // 读取crc
        kv_buf.advance(key_len + value_len);
        let crc = kv_buf.get_u32();
        // 验证crc
        if crc != log_record.get_crc() {
            return Err(Error::InvalidLogRecordCRC);
        }
        Ok(ReadLogRecord {
            record: log_record,
            size: actual_header_size + key_len + value_len + 4,
        })
    }
    pub fn set_write_offset(&self, offset: u64) {
        *self.write_offset.write() = offset;
    }

    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }
}

fn get_data_file_full_path(dir_path: impl AsRef<Path>, file_id: u32) -> PathBuf {
    dir_path
        .as_ref()
        .join(format!("{:09}{}", file_id, DATA_FILE_SUFFIX))
}

#[cfg(test)]
mod tests {
    use crate::data::log_record::LogRecordType;

    use super::*;

    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();
        // println!("dir_path: {}", dir_path.display());
        let data_file = DataFile::new(&dir_path, 0).unwrap();
        assert_eq!(data_file.get_file_id(), 0);
        assert_eq!(data_file.get_write_offset(), 0);

        let data_file = DataFile::new(&dir_path, 0).unwrap();
        assert_eq!(data_file.get_file_id(), 0);
        assert_eq!(data_file.get_write_offset(), 0);

        let data_file = DataFile::new(&dir_path, 1).unwrap();
        assert_eq!(data_file.get_file_id(), 1);
        assert_eq!(data_file.get_write_offset(), 0);
    }

    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();
        let data_file = DataFile::new(&dir_path, 0).unwrap();
        let n_bytes = data_file.write(b"hello").unwrap();
        assert_eq!(n_bytes, 5);
        assert_eq!(data_file.get_write_offset(), 5);

        let n_bytes = data_file.write(b"world").unwrap();
        assert_eq!(n_bytes, 5);
        assert_eq!(data_file.get_write_offset(), 10);

        let n_bytes = data_file.write(b"111").unwrap();
        assert_eq!(n_bytes, 3);
        assert_eq!(data_file.get_write_offset(), 13);

        let n_bytes = data_file.write(b"").unwrap();
        assert_eq!(n_bytes, 0);
        assert_eq!(data_file.get_write_offset(), 13);
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();
        let data_file = DataFile::new(&dir_path, 3).unwrap();
        data_file.write(b"222").unwrap();
        assert!(data_file.sync().is_ok());
    }

    #[test]
    fn test_data_file_read_log_record() {
        let dir_path = std::env::temp_dir();
        println!("dir_path: {}", dir_path.display());
        let data_file = DataFile::new(&dir_path, 4).unwrap();
        assert_eq!(data_file.get_file_id(), 4);

        let log_record = LogRecord {
            key: b"name".to_vec(),
            value: b"bitcask-rs-kv".to_vec(),
            record_type: LogRecordType::NORMAL,
        };
        let write_res = data_file.write(&log_record.encode());
        assert!(write_res.is_ok());
        // 24
        // println!("first write_res: {}", write_res.unwrap());

        // 从起始位置读取
        let read_res = data_file.read_log_record(0);
        assert!(read_res.is_ok());
        let read_res = read_res.unwrap().record;
        assert_eq!(log_record, read_res);

        // 从中间位置读取
        let log_record = LogRecord {
            key: b"name".to_vec(),
            value: b"new-value".to_vec(),
            record_type: LogRecordType::NORMAL,
        };
        let write_res = data_file.write(&log_record.encode());
        assert!(write_res.is_ok());
        // 20
        // println!("second write_res: {}", write_res.unwrap());
        let read_res = data_file.read_log_record(24);
        assert!(read_res.is_ok());
        let read_res = read_res.unwrap().record;
        assert_eq!(log_record, read_res);

        // DELETE类型
        let log_record = LogRecord {
            key: b"name".to_vec(),
            value: Default::default(),
            record_type: LogRecordType::DELETE,
        };
        let write_res = data_file.write(&log_record.encode());
        assert!(write_res.is_ok());
        // 11
        // println!("delete write_res: {}", write_res.unwrap());
        let read_res = data_file.read_log_record(44);
        assert!(read_res.is_ok());
        let read_res = read_res.unwrap().record;
        assert_eq!(log_record, read_res);
    }
}
