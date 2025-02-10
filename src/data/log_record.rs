use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

/// 数据位置索引信息，描述数据存储到了哪个位置
#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

/// log record 结构, 实际写入到数据文件的结构
#[derive(Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType,
}

impl LogRecord {
    /// 编码log record
    /// ```
    ///  +--------------------------------------------------------+
    ///  | record_type | key_len  | value_len | key | value | crc |
    ///  +--------------------------------------------------------+
    ///  | 1B          |var(max:5)| var(max:5)| var | var   | 4B  |
    ///  +--------------------------------------------------------+
    /// ```
    pub fn encode(&self) -> Vec<u8> {
        let (encoded_buf, _) = self.encode_and_get_crc();
        encoded_buf
    }

    pub fn get_crc(&self) -> u32 {
        let (_, crc) = self.encode_and_get_crc();
        crc
    }

    fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
        let mut buf = BytesMut::with_capacity(self.encoded_length());

        // 写入record_type
        buf.put_u8(self.record_type as u8);
        // 写入key长度
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        // 写入value长度
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();
        // 写入key
        buf.put_slice(&self.key);
        // 写入value
        buf.put_slice(&self.value);

        // 计算crc
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&mut buf);
        let crc = hasher.finalize();
        // println!("crc: {}", crc);
        // 写入crc
        buf.put_u32(crc);
        (buf.into(), crc)
    }

    /// 计算编码后的长度
    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + std::mem::size_of::<u32>()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogRecordType {
    NORMAL = 1,
    DELETE = 2,
}

impl From<u8> for LogRecordType {
    fn from(value: u8) -> Self {
        match value {
            1 => LogRecordType::NORMAL,
            2 => LogRecordType::DELETE,
            _ => unreachable!(),
        }
    }
}

/// 从数据文件中读取的log record，包含实际的log record和log record的大小
#[derive(Debug)]
pub struct ReadLogRecord {
    pub record: LogRecord,
    pub size: usize,
}

/// 获取log record header的最大大小
pub fn max_log_record_header_size() -> usize {
    // 1 byte for log record type
    // var bytes for key length
    // var bytes for value length
    // key
    // value
    // 4 bytes for crc
    std::mem::size_of::<u8>() + 2 * length_delimiter_len(u32::MAX as usize)
}

#[cfg(test)]
mod tests {
    use crate::data::data_file::DataFile;

    use super::*;

    #[test]
    fn test_log_record_encode() {
        // 正常记录
        let log_record = LogRecord {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            record_type: LogRecordType::NORMAL,
        };
        let encoded = log_record.encode();
        println!("encoded: {:?}", encoded);
        assert_eq!(log_record.get_crc(), 561450126);
        // value 为空
        let log_record = LogRecord {
            key: b"hello".to_vec(),
            value: vec![],
            record_type: LogRecordType::NORMAL,
        };
        let encoded = log_record.encode();
        println!("encoded: {:?}", encoded);
        assert_eq!(log_record.get_crc(), 193110475);
        // delete 记录
        let log_record = LogRecord {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            record_type: LogRecordType::DELETE,
        };
        let encoded = log_record.encode();
        println!("encoded: {:?}", encoded);
        assert_eq!(log_record.get_crc(), 2629656640);
    }

    #[test]
    fn test_data_file_read_log_record() {
        let dir_path = std::env::temp_dir();
        let data_file = DataFile::new(&dir_path, 500).unwrap();
        assert_eq!(data_file.get_file_id(), 500);

        let log_record = LogRecord {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            record_type: LogRecordType::NORMAL,
        };
        let encoded = log_record.encode();
        data_file.write(&encoded).unwrap();
        let read_log_record = data_file.read_log_record(0).unwrap();
        assert_eq!(read_log_record.record, log_record);
        assert_eq!(read_log_record.size, encoded.len());

        // 从新的位置写入
        let log_record = LogRecord {
            key: b"hello".to_vec(),
            value: b"lyf".to_vec(),
            record_type: LogRecordType::NORMAL,
        };
        let encoded = log_record.encode();
        data_file.write(&encoded).unwrap();
        let read_log_record = data_file.read_log_record(17).unwrap();
        assert_eq!(read_log_record.record, log_record);
        // println!("read_log_record: {:?}", read_log_record);
        assert_eq!(read_log_record.size, encoded.len());

        // 读取删除记录
        let log_record = LogRecord {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            record_type: LogRecordType::DELETE,
        };
        let encoded = log_record.encode();
        data_file.write(&encoded).unwrap();
        let read_log_record = data_file.read_log_record(32).unwrap();
        assert_eq!(read_log_record.record, log_record);
        assert_eq!(read_log_record.size, encoded.len());
    }
}
