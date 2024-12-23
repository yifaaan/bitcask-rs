use prost::length_delimiter_len;

/// 表示实际写入数据文件的一条数据
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

#[derive(PartialEq)]
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
pub struct ReadLogRecord {
    pub record: LogRecord,
    pub size: u64,
}

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        Vec::new()
    }

    pub fn get_crc(&self) -> u32 {
        todo!()
    }
}

/// 获取header的最大长度
pub fn max_log_record_header_size() -> usize {
    std::mem::size_of::<u8>() + length_delimiter_len(std::u32::MAX as usize) * 2
}
