use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    data::{
        data_file::DataFile,
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    error::{Error, Result},
    index,
    options::Options,
};
/// bitcask存储引擎实例
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
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::KeyIsEmpty);
        }
        // 构造LogRecord
        let mut record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
        };

        // 将一条LogRecord追加写入活跃数据文件中
        let log_record_pos = self.append_log_record(&mut record)?;
        // 更新内存索引
        let ok = self.index.put(key.to_vec(), log_record_pos);
        if !ok {
            return Err(Error::IndexUpdateFail);
        }
        Ok(())
    }

    /// 向活跃数据文件添加一条记录，返回记录对应的内存索引信息
    fn append_log_record(&self, record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = self.options.dir_path.clone();
        // 编码写入的数据
        let encoded_record = record.encode();
        let record_len = encoded_record.len() as u64;
        // 获取当前活跃数据文件
        let mut active_file = self.active_file.write();
        // 判断当前活跃数据文件大小是否到达阈值
        if active_file.get_write_offset() + record_len > self.options.data_file_size {
            // 持久化当前活跃数据文件
            active_file.sync()?;
            let current_file_id = active_file.get_file_id();
            // 插入到map中
            let mut older_files = self.older_files.write();
            let older_file = DataFile::new(&dir_path, current_file_id)?;
            older_files.insert(current_file_id, older_file);
            // 打开新的活跃数据文件
            let new_active_file = DataFile::new(&dir_path, current_file_id + 1)?;
            *active_file = new_active_file;
        }
        // 写入数据到活跃数据文件
        let write_offset = active_file.get_write_offset();
        active_file.write(&encoded_record)?;
        // 根据配置决定是立刻否持久化
        if self.options.sync_write {
            active_file.sync()?;
        }
        // 构造这条记录的内存索引信息
        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_offset,
        })
    }
}
