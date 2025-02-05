use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use log::warn;
use parking_lot::RwLock;

use crate::data::data_file::DataFile;
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType};
use crate::error::{Error, Result};
use crate::index;
use crate::options::Options;

const INITIAL_FILE_ID: u32 = 0;

/// 数据库接口
pub struct Engine {
    options: Arc<Options>,
    /// 活跃数据文件
    active_file: Arc<RwLock<DataFile>>,
    /// 旧数据文件
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    /// 内存索引
    index: Box<dyn index::Indexer>,
    /// 数据库启动时，数据文件ID
    file_ids: Vec<u32>,
}

impl Engine {
    /// 打开数据库
    pub fn open(opts: Options) -> Result<Self> {
        // 校验配置项
        check_options(&opts)?;
        // 判断数据库目录是否存在
        let dir_path = opts.dir_path.clone();
        if !dir_path.exists() {
            // 创建数据库目录
            if let Err(e) = std::fs::create_dir_all(&dir_path) {
                warn!("Failed to create database directory: {}", e);
                return Err(Error::FailedToCreateDbDir);
            }
        }
        // 加载目录中的数据文件
        let mut data_files: Vec<DataFile> = load_data_files(&dir_path)?;
        // TODO: fix bug
        // data_files.reverse();
        let file_ids = data_files
            .iter()
            .map(|f| f.get_file_id())
            .collect::<Vec<_>>();
        // 保存旧的数据文件
        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..data_files.len() - 1 {
                let f = data_files.pop().unwrap();
                older_files.insert(f.get_file_id(), f);
            }
        };
        // 获取活跃数据文件
        let active_file = match data_files.pop() {
            Some(f) => f,
            None => DataFile::new(&dir_path, INITIAL_FILE_ID)?,
        };
        let index_type = opts.index_type;
        let engine = Self {
            options: Arc::new(opts),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: Box::new(index::new_indexer(index_type)),
            file_ids,
        };
        // 加载索引
        engine.load_index_from_data_files()?;
        Ok(engine)
    }

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

    /// 从数据库中读取数据
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        if key.is_empty() {
            return Err(Error::KeyIsEmpty);
        }
        // 从内存索引中获取数据位置
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Err(Error::KeyNotFound);
        }
        let pos = pos.unwrap();
        // 从数据文件中读取LogRecord数据
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        let log_record = match active_file.get_file_id() == pos.file_id {
            true => active_file.read_log_record(pos.offset)?.record,
            false => {
                let older_file = older_files.get(&pos.file_id);
                if older_file.is_none() {
                    return Err(Error::DataFileNotFound);
                }
                let older_file = older_file.unwrap();
                older_file.read_log_record(pos.offset)?.record
            }
        };

        // 判断log record类型
        match log_record.record_type {
            LogRecordType::NORMAL => Ok(log_record.value.into()),
            LogRecordType::DELETE => Err(Error::KeyNotFound),
        }
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
            let old_file = DataFile::new(&dir_path, current_file_id)?;
            older_files.insert(current_file_id, old_file);

            // 创建新的活跃数据文件
            let new_active_file = DataFile::new(&dir_path, current_file_id + 1)?;
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

    /// 从数据文件中加载索引
    fn load_index_from_data_files(&self) -> Result<()> {
        if self.file_ids.is_empty() {
            return Ok(());
        }
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        // 遍历数据文件
        for (i, file_id) in self.file_ids.iter().enumerate() {
            let mut offset: u64 = 0;
            // 遍历数据文件中的数据
            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    false => {
                        let data_file = older_files.get(file_id).unwrap();
                        data_file.read_log_record(offset)
                    }
                };
                let (log_record, size) = match log_record_res {
                    Ok(rc) => (rc.record, rc.size),
                    Err(e) => {
                        // 读取数据文件结束, 退出循环, 继续遍历下一个数据文件
                        if e == Error::ReadDataFileEOF {
                            break;
                        }
                        return Err(e);
                    }
                };
                // 更新内存索引
                let pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };
                match log_record.record_type {
                    LogRecordType::NORMAL => {
                        self.index.put(log_record.key.clone(), pos);
                    }
                    // 删除数据
                    LogRecordType::DELETE => {
                        self.index.delete(log_record.key.clone());
                    }
                }
                // 更新偏移量
                offset += size as u64;
            }
            // 最后一个数据文件处理完了，更新活跃数据文件的偏移量
            if i == self.file_ids.len() - 1 {
                active_file.set_write_offset(offset);
            }
        }
        Ok(())
    }
}

/// 校验配置项
fn check_options(opts: &Options) -> Result<()> {
    if opts.dir_path.to_str().is_none() || opts.dir_path.to_str().unwrap().is_empty() {
        return Err(Error::InvalidDbDir);
    }
    if opts.data_file_size <= 0 {
        return Err(Error::InvalidDataFileSize);
    }
    Ok(())
}

/// 加载目录中的数据文件
fn load_data_files(dir_path: impl AsRef<Path>) -> Result<Vec<DataFile>> {
    let mut file_ids = Vec::new();
    let mut data_files = Vec::new();
    for entry in std::fs::read_dir(dir_path.as_ref()).map_err(|_| Error::FailedToReadDir)? {
        let entry = entry.map_err(|_| Error::FailedToReadDirEntry)?;
        let file_os_name = entry.file_name();
        let file_name = file_os_name.to_str().unwrap();
        // 判断文件名是否是以.data结尾
        if file_name.ends_with(crate::data::data_file::DATA_FILE_SUFFIX) {
            let (id, _) = file_name.split_once(".").unwrap();
            let id = id.parse::<u32>().map_err(|_| Error::FailedToParseFileId)?;
            file_ids.push(id);
        }
    }
    if file_ids.is_empty() {
        return Ok(data_files);
    }

    file_ids.sort();
    // 根据file_ids加载数据文件
    for id in file_ids.iter() {
        let data_file =
            DataFile::new(dir_path.as_ref(), *id).map_err(|_| Error::FailedToCreateDataFile)?;
        data_files.push(data_file);
    }
    Ok(data_files)
}
