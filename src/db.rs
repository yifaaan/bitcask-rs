use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::data::data_file::DataFile;
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType};
use crate::error::{Error, Result};
use crate::index;
use crate::options::Options;

/// 数据库接口
pub struct Engine {
    options: Arc<Options>,
    /// 活跃数据文件
    active_file: Arc<RwLock<DataFile>>,
    /// 旧数据文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    /// 内存索引
    index: Box<dyn index::Indexer>,
}

impl Engine {
    /// 向数据库中写入数据, key不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::KeyIsEmpty);
        }

        // 构造log record
        let mut record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: LogRecordType::NORMAL,
        };
        // 追加写入活跃数据文件
        let pos = self.append_log_record(&mut record)?;

        // 更新内存索引
        if !self.index.put(key.to_vec(), pos) {
            return Err(Error::FailedToUpdateIndex);
        }
        Ok(())
    }

    /// 追加写入活跃数据文件
    fn append_log_record(&self, record: &mut LogRecord) -> Result<LogRecordPos> {
        // 数据库目录
        let dir_path = self.options.dir_path.clone();
        // 编码输入数据
        let encoded_data = record.encode();
        let encoded_len = encoded_data.len() as u64;
        // 获取活跃数据文件
        let mut active_file = self.active_file.write();
        // 如果活跃数据文件满了，则创建新的活跃数据文件
        if active_file.get_write_offset() + encoded_len > self.options.data_file_size {
            // 持久化当前活跃数据文件
            active_file.sync()?;

            // 将当前活跃数据文件移动到旧数据文件中
            let current_file_id = active_file.get_file_id();
            let mut older_files = self.older_files.write();
            let old_file = DataFile::new(dir_path.as_ref(), current_file_id)?;
            older_files.insert(current_file_id, old_file);

            // 创建新的活跃数据文件
            let new_active_file = DataFile::new(dir_path.as_ref(), current_file_id + 1)?;
            *active_file = new_active_file;
        }
        // 写入数据到活跃数据文件
        let write_offset = active_file.get_write_offset();
        active_file.write(&encoded_data)?;

        // 根据配置决定是否持久化
        if self.options.sync_write {
            active_file.sync()?;
        }

        // 返回活跃数据文件的内存索引信息
        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_offset,
        })
    }
}
