use bytes::{BufMut, BytesMut};
use prost::{encode_length_delimiter, length_delimiter_len};

/// 表示实际写入数据文件的一条数据
#[derive(Debug)]
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

/// 数据位置索引信息，描述数据存储到了哪个位置
#[derive(Clone, Copy)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum LogRecordType {
    NORMAL = 1,
    DELETED = 2,
}

impl From<u8> for LogRecordType {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::NORMAL,
            2 => Self::DELETED,
            _ => unreachable!(),
        }
    }
}

/// 从数据文件读取的log_record和它的size
#[derive(Debug)]
pub struct ReadLogRecord {
    pub record: LogRecord,
    pub size: u64,
}

impl LogRecord {
    /// 对LogRecord编码，返回编码的结果
    ///
    /// +-----------+-----------+-----------+-----------+-----------+-----------+
    /// |   type    | key size  | value size|   key     |    value  |   CRC     |
    /// +-----------+-----------+-----------+-----------+-----------+-----------+
    ///      1B          Max:5B    Max:5B       vary         vary        4B
    ///
    pub fn encode(&self) -> Vec<u8> {
        let (encoded_buf, _) = self.encode_and_get_crc();
        encoded_buf
    }

    /// 获取LogRecord编码后的CRC
    pub fn get_crc(&self) -> u32 {
        let (_, crc) = self.encode_and_get_crc();
        crc
    }

    /// LogRecord编码后的长度
    fn encoded_length(&self) -> usize {
        std::mem::size_of::<u8>()
            + length_delimiter_len(self.key.len())
            + length_delimiter_len(self.value.len())
            + self.key.len()
            + self.value.len()
            + 4
    }

    fn encode_and_get_crc(&self) -> (Vec<u8>, u32) {
        // 存放编码结果
        let mut buf = BytesMut::with_capacity(self.encoded_length());
        // type
        buf.put_u8(self.rec_type as u8);
        // key size
        encode_length_delimiter(self.key.len(), &mut buf).unwrap();
        // value size
        encode_length_delimiter(self.value.len(), &mut buf).unwrap();
        // key
        buf.extend_from_slice(&self.key);
        // value
        buf.extend_from_slice(&self.value);
        // CRC
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let crc = hasher.finalize();
        buf.put_u32(crc);
        // println!("crc: {crc}");
        (buf.into(), crc)
    }
}

/// 获取header的最大长度
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_record_encode() {
        // 正常的LogRecord编码
        let recd = LogRecord {
            key: "name".into(),
            value: "bitcask-rs".into(),
            rec_type: LogRecordType::NORMAL,
        };
        let ec = recd.encode();
        assert!(ec.len() > 5);
        assert_eq!(recd.get_crc(), 1020360578);

        // value为空
        let recd = LogRecord {
            key: "name".into(),
            value: Default::default(),
            rec_type: LogRecordType::NORMAL,
        };
        let ec = recd.encode();
        assert!(ec.len() > 5);
        assert_eq!(recd.get_crc(), 3756865478);

        // 类型为Deleted
        let recd = LogRecord {
            key: "name".into(),
            value: "bitcask-rs".into(),
            rec_type: LogRecordType::DELETED,
        };
        let ec = recd.encode();
        assert!(ec.len() > 5);
        assert_eq!(recd.get_crc(), 1867197446);
    }
}
