use std::{collections::HashMap, path::Path, sync::Arc};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{
    data::{
        data_file::{DataFile, DATA_FILE_NAME_SUFFIX},
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    error::{Error, Result},
    index::{self, new_indexer},
    options::Options,
};

const INITIAL_FILE_ID: u32 = 0;

/// bitcask存储引擎实例
pub struct Engine {
    options: Arc<Options>,
    /// 活跃数据文件
    active_file: Arc<RwLock<DataFile>>,
    /// 旧数据文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    /// 内存索引
    pub(crate) index: Box<dyn index::Indexer>,
    /// 文件id
    file_ids: Vec<u32>,
}

impl Engine {
    pub fn open(opts: Options) -> Result<Self> {
        // 校验配置项
        if let Some(e) = check_options(&opts) {
            return Err(e);
        }
        let option = opts.clone();
        // 判断数据目录是否存在，不存在时需要创建
        let dir_path = option.dir_path.clone();
        if !dir_path.is_dir() {
            if let Err(e) = std::fs::create_dir(&dir_path) {
                log::warn!("create database directory err: {}", e);
                return Err(Error::FailedToCreateDatabaseDir);
            }
        }
        // 加载目录中的数据文件
        let mut data_files = load_data_files(&dir_path)?;
        // 设置file id
        let mut file_ids = Vec::new();
        for v in &data_files {
            file_ids.push(v.get_file_id());
        }

        // 将旧数据文件保存到older_files
        // 将旧的数据文件放到后面
        data_files.reverse();
        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..=data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }
        // 当前活跃数据文件
        let active_file = match data_files.pop() {
            Some(v) => v,
            None => DataFile::new(&dir_path, INITIAL_FILE_ID)?,
        };

        // 构造存储引擎实例
        let engine = Self {
            options: Arc::new(opts),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: Box::new(new_indexer(option.index_type)),
            file_ids,
        };
        engine.load_index_from_data_files()?;
        Ok(engine)
    }

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
    /// 根据key获得对应的value
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Error::KeyIsEmpty);
        }
        // 从内存索引获取key对应的位置信息
        let log_record_pos = self.index.get(key.to_vec());
        if log_record_pos.is_none() {
            return Err(Error::KeyNotFound);
        }
        let log_record_pos = log_record_pos.unwrap();
        self.get_value_by_position(&log_record_pos)
    }

    /// 根据偏移位置读取value
    pub(crate) fn get_value_by_position(&self, log_record_pos: &LogRecordPos) -> Result<Bytes> {
        let log_record;
        // 先尝试读取活跃数据文件
        if self.active_file.read().get_file_id() == log_record_pos.file_id {
            log_record = self
                .active_file
                .read()
                .read_log_record(log_record_pos.offset)?
                .record;
        } else {
            let older_files = self.older_files.read();
            let data_file = older_files.get(&log_record_pos.file_id);
            // 找不到对应的数据文件
            if data_file.is_none() {
                return Err(Error::DataFileNotFound);
            }
            log_record = data_file
                .unwrap()
                .read_log_record(log_record_pos.offset)?
                .record;
        }
        // 判断record类型
        if log_record.rec_type == LogRecordType::DELETED {
            return Err(Error::KeyNotFound);
        }
        Ok(log_record.value.into())
    }

    /// 从数据文件中加载内存索引
    /// 遍历数据文件中的内容，依次处理其中的记录
    fn load_index_from_data_files(&self) -> Result<()> {
        if self.file_ids.is_empty() {
            return Ok(());
        }
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();

        // 加载每个数据文件内容
        for (i, file_id) in self.file_ids.iter().enumerate() {
            let mut offset = 0;
            loop {
                let log_record = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => {
                        let data_file = older_files.get(file_id).unwrap();
                        data_file.read_log_record(offset)
                    }
                };
                let (log_record, size) = match log_record {
                    Ok(v) => (v.record, v.size),
                    Err(e) => {
                        if e == Error::ReadDataFileEof {
                            // 读到文件末尾,继续读下一个文件
                            break;
                        } else {
                            return Err(e);
                        }
                    }
                };
                // 构建内存索引
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };
                let ok = match log_record.rec_type {
                    LogRecordType::NORMAL => self.index.put(log_record.key.clone(), log_record_pos),
                    LogRecordType::DELETED => self.index.delete(log_record.key.clone()),
                };
                if !ok {
                    return Err(Error::IndexUpdateFail);
                }
                // 递增offset，增加量为读取的record的大小
                offset += size;
            }
            // 设置活跃文件的offset
            if i == self.file_ids.len() - 1 {
                active_file.set_write_offset(offset);
            }
        }
        Ok(())
    }

    /// 根据key删除对应的数据
    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::KeyIsEmpty);
        }
        // 从内存索引查询key是否存在
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Ok(());
        }
        // 构造log_record
        let mut log_record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::DELETED,
        };
        // 将log_record追加写入数据文件中
        self.append_log_record(&mut log_record)?;
        // 更新内存索引
        let ok = self.index.delete(key.to_vec());
        if !ok {
            return Err(Error::IndexUpdateFail);
        }
        Ok(())
    }

    /// 关闭数据库
    pub fn close(&self) -> Result<()> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }

    /// 持久化当前活跃文件
    pub fn sync(&self) -> Result<()> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }
}

fn check_options(opts: &Options) -> Option<Error> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().len() == 0 {
        return Some(Error::DirPathIsEmpty);
    }
    if opts.data_file_size <= 0 {
        return Some(Error::DataFileSizeTooSmall);
    }
    None
}

fn load_data_files(dir_path: impl AsRef<Path>) -> Result<Vec<DataFile>> {
    let dir = std::fs::read_dir(dir_path.as_ref());
    if dir.is_err() {
        return Err(Error::FailedToReadDatabaseDir);
    }

    let mut file_ids = Vec::new();
    let mut data_files = Vec::new();
    for file in dir.unwrap() {
        if let Ok(entry) = file {
            // 获取文件名
            let file_name_os_str = entry.file_name();
            let file_name = file_name_os_str.to_str().unwrap();
            // 判断文件名是否以.data结尾
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let split_name: Vec<&str> = file_name.split('.').collect();
                let file_id = match split_name[0].parse::<u32>() {
                    Ok(fid) => fid,
                    Err(_) => {
                        return Err(Error::DataDirectoryCorrupted);
                    }
                };
                file_ids.push(file_id);
            }
        }
    }

    if file_ids.is_empty() {
        return Ok(data_files);
    }

    // 排序文件id，从小到大
    file_ids.sort();
    // 依次打开数据文件
    for file_id in &file_ids {
        let data_file = DataFile::new(dir_path.as_ref(), *file_id)?;
        data_files.push(data_file);
    }
    Ok(data_files)
}
