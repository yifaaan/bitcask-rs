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
}

pub enum LogRecordType {
    NORMAL = 1,
    DELETE = 2,
}

/// 从数据文件中读取的log record，包含实际的log record和log record的大小
pub struct ReadLogRecord {
    pub record: LogRecord,
    pub size: u64,
}
