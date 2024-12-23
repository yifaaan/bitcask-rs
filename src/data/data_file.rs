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
        self.io_manager.write(buf)
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
