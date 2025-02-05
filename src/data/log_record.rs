use prost::length_delimiter_len;

/// 数据位置索引信息，描述数据存储到了哪个位置
#[derive(Clone, Copy)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

/// log record 结构, 实际写入到数据文件的结构
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) record_type: LogRecordType,
}

impl LogRecord {
    pub fn encode(&self) -> Vec<u8> {
        todo!()
    }

    pub fn get_crc(&self) -> u32 {
        todo!()
    }
}

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
