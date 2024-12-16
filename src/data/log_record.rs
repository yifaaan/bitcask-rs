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

impl LogRecord {
    pub fn encode(&mut self) -> Vec<u8> {
        Vec::new()
    }
}
