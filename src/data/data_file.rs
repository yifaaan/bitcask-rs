use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Buf, BytesMut};
use parking_lot::RwLock;
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::data::log_record::max_log_record_header_size;
use crate::error::{Error, Result};
use crate::fio::{self, new_io_manager};

use super::log_record::{LogRecord, ReadLogRecord};

pub const DATA_FILE_NAME_SUFFIX: &'static str = ".data";
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
    /// 创建新数据文件
    pub fn new(dir_path: impl AsRef<Path>, file_id: u32) -> Result<Self> {
        // 构造文件名称
        let file_name = get_data_file_name(dir_path, file_id);
        // io_manager
        let io_manager = new_io_manager(file_name)?;
        Ok({
            Self {
                file_id: Arc::new(RwLock::new(file_id)),
                write_offset: Arc::new(RwLock::new(0)),
                io_manager: Box::new(io_manager),
            }
        })
    }
    /// 获得数据文件的当前写偏移
    pub fn get_write_offset(&self) -> u64 {
        *self.write_offset.read()
    }
    /// 设置数据文件的写偏移
    pub fn set_write_offset(&self, offset: u64) {
        *self.write_offset.write() = offset;
    }
    /// 获得数据文件id
    pub fn get_file_id(&self) -> u32 {
        *self.file_id.read()
    }
    /// 写入数据文件
    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        let n_bytes = self.io_manager.write(buf)?;
        // 更新write_off
        *self.write_offset.write() += n_bytes as u64;
        Ok(n_bytes)
    }
    /// 持久化数据文件
    pub fn sync(&self) -> Result<()> {
        self.io_manager.sync()
    }
    /// 从offset处开始，读取一条LogRecord
    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        // 读取header部分
        let mut header = BytesMut::zeroed(max_log_record_header_size());
        self.io_manager.read(&mut header, offset)?;
        // 取出header中的type
        let rec_type = header.get_u8();
        // 取出header中key_size和val_size
        let key_size = decode_length_delimiter(&mut header).unwrap();
        let val_size = decode_length_delimiter(&mut header).unwrap();
        if key_size == 0 && val_size == 0 {
            // EOF
            return Err(Error::ReadDataFileEof);
        }
        // 得到实际header的大小
        let actual_header_size =
            1 + length_delimiter_len(key_size) + length_delimiter_len(val_size);
        // 读取key、val和CRC
        let mut key_val = BytesMut::zeroed(key_size + val_size + 4);
        self.io_manager
            .read(&mut key_val, offset + actual_header_size as u64)?;
        // 构造log_record
        let log_record = LogRecord {
            key: key_val.get(..key_size).unwrap().to_vec(),
            value: key_val.get(key_size..key_val.len() - 4).unwrap().to_vec(),
            rec_type: rec_type.into(),
        };
        key_val.advance(key_size + val_size);
        let crc = key_val.get_u32();
        // 对比crc
        if crc != log_record.get_crc() {
            return Err(Error::InvalidLogRecordCrc);
        }
        Ok(ReadLogRecord {
            record: log_record,
            size: (actual_header_size + key_size + val_size + 4) as u64,
        })
    }
}

fn get_data_file_name(path: impl AsRef<Path>, file_id: u32) -> PathBuf {
    path.as_ref()
        .join(std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();
        let data_file = DataFile::new(&dir_path, 0).unwrap();

        assert_eq!(data_file.get_file_id(), 0);
        println!("{:?}", dir_path);
    }

    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();
        let data_file = DataFile::new(&dir_path, 1).unwrap();

        assert_eq!(data_file.get_file_id(), 1);

        let write = data_file.write("aaa".as_bytes()).unwrap();
        assert_eq!(write, 3);

        let write = data_file.write("bbb".as_bytes()).unwrap();
        assert_eq!(write, 3);

        assert_eq!(data_file.get_write_offset(), 6);
        println!("{:?}", dir_path);
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();
        let data_file = DataFile::new(&dir_path, 2).unwrap();

        assert_eq!(data_file.get_file_id(), 2);

        let write = data_file.write("aaa".as_bytes()).unwrap();
        assert_eq!(write, 3);
        assert!(data_file.sync().is_ok());
    }

    #[test]
    fn test_data_file_read_log_record() {
        let dir_path = std::env::temp_dir();
        println!("dir_path: {}", dir_path.display());
        let data_file = DataFile::new(&dir_path, 100).unwrap();
        assert_eq!(data_file.get_file_id(), 100);

        let record = LogRecord {
            key: "name".into(),
            value: "bitcast-rs".into(),
            rec_type: crate::data::log_record::LogRecordType::NORMAL,
        };
        let ecd = record.encode();
        // println!("ecd len: {}", ecd.len()); 21
        let write_res = data_file.write(&ecd);
        assert!(write_res.is_ok());

        // 从文件开头读取记录
        let read_res = data_file.read_log_record(0);
        assert!(read_res.is_ok());
        let read_res = read_res.unwrap().record;
        // println!("{:?}", read_res);
        assert_eq!(read_res.key, record.key);
        assert_eq!(read_res.value, record.value);
        assert_eq!(read_res.rec_type, record.rec_type);

        // 从其他位置读取记录
        let record = LogRecord {
            key: "key1".into(),
            value: "value1".into(),
            rec_type: crate::data::log_record::LogRecordType::NORMAL,
        };
        let ecd = record.encode();
        // println!("ecd len: {}", ecd.len()); 17
        let write_res = data_file.write(&ecd);
        assert!(write_res.is_ok());
        let read_res = data_file.read_log_record(21);
        assert!(read_res.is_ok());
        let read_res = read_res.unwrap().record;
        // println!("{:?}", read_res);
        assert_eq!(read_res.key, record.key);
        assert_eq!(read_res.value, record.value);
        assert_eq!(read_res.rec_type, record.rec_type);

        // 类型是Deleted
        let record = LogRecord {
            key: "key2".into(),
            value: "value2".into(),
            rec_type: crate::data::log_record::LogRecordType::DELETED,
        };
        let ecd = record.encode();
        // println!("ecd len: {}", ecd.len()); 17
        let write_res = data_file.write(&ecd);
        assert!(write_res.is_ok());
        let read_res = data_file.read_log_record(38);
        assert!(read_res.is_ok());
        let read_res = read_res.unwrap().record;
        // println!("{:?}", read_res);
        assert_eq!(read_res.key, record.key);
        assert_eq!(read_res.value, record.value);
        assert_eq!(read_res.rec_type, record.rec_type);
    }
}
