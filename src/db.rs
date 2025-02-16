use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use log::warn;
use parking_lot::{Mutex, RwLock};

use crate::batch::{log_record_key_with_seq_num, parse_log_record_key, NON_TRANSACTION_SEQ_NUM};
use crate::data::data_file::DataFile;
use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType, TransactionRecord};
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
    pub(crate) index: Box<dyn index::Indexer>,
    /// 数据库启动时，数据文件ID
    file_ids: Vec<u32>,
    /// 批量写操作的锁
    pub(crate) batch_commit_lock: Mutex<()>,
    /// 全局事务编号
    pub(crate) seq_num: Arc<std::sync::atomic::AtomicUsize>,
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
        // ID大的数据文件越新
        data_files.reverse();
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
            batch_commit_lock: Mutex::new(()),
            seq_num: Arc::new(std::sync::atomic::AtomicUsize::new(1)),
        };
        // 加载索引，并更新事务序列号
        let seq_num = engine.load_index_from_data_files()?;
        if seq_num > 0 {
            engine
                .seq_num
                .store(seq_num, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(engine)
    }

    /// 向数据库中写入数据, key不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::KeyIsEmpty);
        }

        // 构造log record, 事务编号为0表示非事务写入的数据
        let record = LogRecord {
            key: log_record_key_with_seq_num(&key, NON_TRANSACTION_SEQ_NUM),
            value: value.to_vec(),
            record_type: LogRecordType::NORMAL,
        };
        // 追加写入活跃数据文件
        let pos = self.append_log_record(&record)?;

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
        if let Some(pos) = self.index.get(key.to_vec()) {
            self.get_value_by_position(&pos)
        } else {
            Err(Error::KeyNotFound)
        }
    }

    pub fn close(&self) -> Result<()> {
        self.active_file.read().sync()
    }

    pub fn sync(&self) -> Result<()> {
        self.active_file.read().sync()
    }

    pub fn get_value_by_position(&self, pos: &LogRecordPos) -> Result<Bytes> {
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
            _ => unreachable!(),
        }
    }

    /// 从数据库中删除数据
    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::KeyIsEmpty);
        }

        // 从内存索引中获取数据位置
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Ok(());
        }
        // 构造删除的log record, 事务编号为0表示非事务写入的数据
        let log_record = LogRecord {
            key: log_record_key_with_seq_num(&key, NON_TRANSACTION_SEQ_NUM),
            value: Default::default(),
            record_type: LogRecordType::DELETE,
        };
        let pos = self.append_log_record(&log_record)?;
        // 更新内存索引
        if !self.index.delete(key.to_vec()) {
            return Err(Error::FailedToUpdateIndex);
        }
        Ok(())
    }

    /// 追加写入活跃数据文件
    pub(crate) fn append_log_record(&self, record: &LogRecord) -> Result<LogRecordPos> {
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
    fn load_index_from_data_files(&self) -> Result<usize> {
        let mut current_seq_num = NON_TRANSACTION_SEQ_NUM;
        if self.file_ids.is_empty() {
            return Ok(current_seq_num);
        }
        // 事务批量写入的数据，暂存到内存中
        // seq_num -> records
        let mut transaction_batch_records: HashMap<usize, Vec<TransactionRecord>> = HashMap::new();

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
                let (mut log_record, size) = match log_record_res {
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
                // 解析key，返回key和事务编号
                let (key, seq_num) = parse_log_record_key(&log_record.key).unwrap();
                // 非事务写入的数据，直接更新内存索引
                if seq_num == NON_TRANSACTION_SEQ_NUM {
                    self.update_index(&key, log_record.record_type, pos);
                } else {
                    // 事务批量写入的数据，暂存到内存中

                    // 表示一个事务的结束
                    if log_record.record_type == LogRecordType::TXNFINISHED {
                        // 当前事务的所有数据
                        let records = transaction_batch_records.get(&seq_num).unwrap();
                        // 更新内存索引
                        records.iter().for_each(|trans_record| {
                            self.update_index(
                                &trans_record.record.key,
                                trans_record.record.record_type,
                                trans_record.pos,
                            );
                        });
                        // 移除当前事务的所有数据
                        transaction_batch_records.remove(&seq_num);
                    } else {
                        // 事务中提交的数据，更新key
                        log_record.key = key;
                        // 暂存到内存中
                        transaction_batch_records
                            .entry(seq_num)
                            .or_insert(Vec::new())
                            .push(TransactionRecord {
                                record: log_record,
                                pos,
                            });
                    }
                }

                // 更新事务序列号
                if seq_num > current_seq_num {
                    current_seq_num = seq_num;
                }

                // 更新偏移量
                offset += size as u64;
            }
            // 最后一个数据文件处理完了，更新活跃数据文件的偏移量
            if i == self.file_ids.len() - 1 {
                active_file.set_write_offset(offset);
            }
        }
        Ok(current_seq_num)
    }

    fn update_index(&self, key: &[u8], record_type: LogRecordType, pos: LogRecordPos) {
        match record_type {
            LogRecordType::NORMAL => {
                self.index.put(key.to_vec(), pos);
            }
            // 删除数据
            LogRecordType::DELETE => {
                self.index.delete(key.to_vec());
            }
            _ => {}
        }
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::util::rand_kv::{get_test_key, get_test_value};

    use super::*;

    #[test]
    fn test_engine_put() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-put");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // 正常put一条数据
        let put_res = engine.put(get_test_key(11), get_test_value(11));
        assert!(put_res.is_ok());
        let get_res = engine.get(get_test_key(11));
        assert!(get_res.is_ok());
        assert!(get_res.unwrap().len() > 0);

        // 重复put key相同的数据
        let put_res = engine.put(get_test_key(22), get_test_value(22));
        assert!(put_res.is_ok());
        let put_res = engine.put(get_test_key(22), Bytes::from("a new value"));
        assert!(put_res.is_ok());
        let get_res = engine.get(get_test_key(22));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), Bytes::from("a new value"));

        // put空key
        let put_res = engine.put(Bytes::new(), get_test_value(123));
        assert!(put_res.err().unwrap() == Error::KeyIsEmpty);

        // put空value
        let put_res = engine.put(get_test_key(33), Bytes::new());
        assert!(put_res.is_ok());
        let get_res = engine.get(get_test_key(33));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), Bytes::new());

        // 写到数据文件进行了转换
        for i in 0..=1000000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
        }

        // 重启数据库
        std::mem::drop(engine);

        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        let put_res = engine.put(get_test_key(55), get_test_value(55));
        assert!(put_res.is_ok());
        let get_res = engine.get(get_test_key(55));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), get_test_value(55));

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.dir_path).expect("failed to remove test dir");
    }

    #[test]
    fn test_engine_get() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-get");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // 正常get一条数据
        let put_res = engine.put(get_test_key(111), get_test_value(111));
        assert!(put_res.is_ok());
        let get_res = engine.get(get_test_key(111));
        assert!(get_res.is_ok());
        assert!(get_res.unwrap().len() > 0);

        // 读一个不存在的key
        let get_res = engine.get(Bytes::from("not_exist_key"));
        assert!(get_res.err().unwrap() == Error::KeyNotFound);

        // 值被重复put后再读取
        let put_res = engine.put(get_test_key(222), get_test_value(222));
        assert!(put_res.is_ok());
        let put_res = engine.put(get_test_key(222), Bytes::from("a new value"));
        assert!(put_res.is_ok());
        let get_res = engine.get(get_test_key(222));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), Bytes::from("a new value"));

        // 值被删除后再get
        let put_res = engine.put(get_test_key(333), get_test_value(333));
        assert!(put_res.is_ok());
        let delete_res = engine.delete(get_test_key(333));
        assert!(delete_res.is_ok());
        let get_res = engine.get(get_test_key(333));
        assert!(get_res.err().unwrap() == Error::KeyNotFound);

        // 转换为旧的数据文件，从旧的数据文件获取value
        for i in 500..=1000000 {
            let put_res = engine.put(get_test_key(i), get_test_value(i));
            assert!(put_res.is_ok());
        }
        let get_res = engine.get(get_test_key(555));
        assert!(get_res.is_ok());
        assert_eq!(get_res.unwrap(), get_test_value(555));

        // 重启数据库后，之前写入的数据都能读到
        std::mem::drop(engine);
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        let get_res = engine.get(get_test_key(111));
        assert_eq!(get_res.unwrap(), get_test_value(111));
        let get_res = engine.get(get_test_key(222));
        assert_eq!(get_res.unwrap(), Bytes::from("a new value"));
        let get_res = engine.get(get_test_key(333));
        assert!(get_res.err().unwrap() == Error::KeyNotFound);
        let get_res = engine.get(get_test_key(555));
        assert_eq!(get_res.unwrap(), get_test_value(555));

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.dir_path).expect("failed to remove test dir");
    }

    #[test]
    fn test_engine_delete() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-delete");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // 正常删除一条数据
        let put_res = engine.put(get_test_key(11), get_test_value(11));
        assert!(put_res.is_ok());
        let delete_res = engine.delete(get_test_key(11));
        assert!(delete_res.is_ok());
        let get_res = engine.get(get_test_key(11));
        assert!(get_res.err().unwrap() == Error::KeyNotFound);

        // 删除一个不存在的key
        let delete_res = engine.delete(get_test_key(22));
        assert!(delete_res.is_ok());

        // 删除空key
        let delete_res = engine.delete(Bytes::new());
        assert!(delete_res.err().unwrap() == Error::KeyIsEmpty);

        // 删除后再次put
        let put_res = engine.put(get_test_key(33), get_test_value(33));
        assert!(put_res.is_ok());
        let delete_res = engine.delete(get_test_key(33));
        assert!(delete_res.is_ok());
        let put_res = engine.put(get_test_key(33), Bytes::from("a new value"));
        assert!(put_res.is_ok());
        let get_res = engine.get(get_test_key(33));
        assert_eq!(get_res.unwrap(), Bytes::from("a new value"));

        // 重启数据库，再次put
        std::mem::drop(engine);
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        let get_res = engine.get(get_test_key(11));
        assert!(get_res.err().unwrap() == Error::KeyNotFound);
        let get_res = engine.get(get_test_key(33));
        assert_eq!(get_res.unwrap(), Bytes::from("a new value"));

        // 删除测试的文件夹
        std::fs::remove_dir_all(opts.dir_path).expect("failed to remove test dir");
    }

    #[test]
    fn test_engine_close() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-close");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let put_res = engine.put(get_test_key(11), get_test_value(11));
        assert!(put_res.is_ok());
        let close_res = engine.close();
        assert!(close_res.is_ok());
        std::fs::remove_dir_all(opts.dir_path).expect("failed to remove test dir");
    }

    #[test]
    fn test_engine_sync() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-sync");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let put_res = engine.put(get_test_key(11), get_test_value(11));
        assert!(put_res.is_ok());
        let sync_res = engine.sync();
        assert!(sync_res.is_ok());
        std::fs::remove_dir_all(opts.dir_path).expect("failed to remove test dir");
    }
}
