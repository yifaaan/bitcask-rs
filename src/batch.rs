use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use parking_lot::RwLock;

use crate::data::log_record::{LogRecord, LogRecordPos, LogRecordType};
use crate::db::Engine;
use crate::error::{Error, Result};
use crate::options::WriteOptions;

const TXN_FINISH_KEY: &'static [u8] = b"txn-finish";
pub(crate) const NON_TRANSACTION_SEQ_NUM: usize = 0;

/// 批量写操作，保证原子性
pub struct WriteBatch<'a> {
    pending_writes: Arc<RwLock<HashMap<Vec<u8>, LogRecord>>>,
    engine: &'a Engine,
    opts: WriteOptions,
}

impl<'a> WriteBatch<'a> {
    /// 写入数据
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::KeyIsEmpty);
        }
        // 暂存数据
        let log_record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            record_type: crate::data::log_record::LogRecordType::NORMAL,
        };
        // 写入batch
        let mut pending_writes = self.pending_writes.write();
        pending_writes.insert(key.to_vec(), log_record);
        Ok(())
    }

    /// 删除数据
    pub fn delete(&self, key: Bytes) -> Result<()> {
        if key.is_empty() {
            return Err(Error::KeyIsEmpty);
        }
        let mut pending_writes = self.pending_writes.write();
        if self.engine.index.get(key.to_vec()).is_none() {
            // 如果key不在索引中，但在batch中,需要从batch中删掉
            if pending_writes.contains_key(key.as_ref()) {
                pending_writes.remove(key.as_ref());
            }
            return Ok(());
        }
        // 暂存数据
        let log_record = LogRecord {
            key: key.to_vec(),
            value: Default::default(),
            record_type: crate::data::log_record::LogRecordType::DELETE,
        };
        // 写入batch
        pending_writes.insert(key.to_vec(), log_record);
        Ok(())
    }

    /// 提交批量写操作，将数据写入文件并更新内存索引
    pub fn commit(&self) -> Result<()> {
        let mut pending_writes = self.pending_writes.write();
        if pending_writes.is_empty() {
            return Ok(());
        }
        if pending_writes.len() > self.opts.max_batch_size {
            return Err(Error::BatchTooLarge);
        }
        // 加锁保证事务串行化
        let _lock = self.engine.batch_commit_lock.lock();
        // 获取全局事务编号
        let seq_num = self
            .engine
            .seq_num
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let mut positions = HashMap::with_capacity(pending_writes.len());
        for (_, rec) in pending_writes.iter() {
            let log_record = LogRecord {
                key: log_record_key_with_seq_num(&rec.key, seq_num),
                value: rec.value.clone(),
                record_type: rec.record_type,
            };
            let pos = self.engine.append_log_record(&log_record)?;
            positions.insert(rec.key.clone(), pos);
        }
        // 写入最后一条标识事务完成的数据
        let finish_record = LogRecord {
            key: log_record_key_with_seq_num(TXN_FINISH_KEY, seq_num),
            value: Default::default(),
            record_type: LogRecordType::TXNFINISHED,
        };
        self.engine.append_log_record(&finish_record)?;
        // 持久化批量写入
        if self.opts.sync_writes {
            self.engine.sync()?;
        }
        // 更新内存索引
        pending_writes
            .iter()
            .map(|(key, rec)| {
                // 正常的记录,更新内存索引
                if rec.record_type == LogRecordType::NORMAL {
                    let pos = positions.get(key).unwrap();
                    self.engine.index.put(rec.key.clone(), *pos);
                } else if rec.record_type == LogRecordType::DELETE {
                    self.engine.index.delete(rec.key.clone());
                }
            })
            .count();
        // 清空batch
        pending_writes.clear();
        Ok(())
    }
}

impl Engine {
    /// 创建一个批量写操作
    pub fn new_write_batch(&self, opts: WriteOptions) -> Result<WriteBatch> {
        Ok(WriteBatch {
            pending_writes: Arc::new(RwLock::new(HashMap::new())),
            engine: self,
            opts,
        })
    }
}

/// 为key添加事务编号
pub(crate) fn log_record_key_with_seq_num(key: &[u8], seq_num: usize) -> Vec<u8> {
    let mut encoded_key = BytesMut::new();
    prost::encode_length_delimiter(seq_num, &mut encoded_key).unwrap();
    encoded_key.extend_from_slice(key);
    encoded_key.into()
}

/// 解析key，返回key和事务编号
pub(crate) fn parse_log_record_key(key: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut buf = BytesMut::from(key);
    let seq_num = prost::decode_length_delimiter(&mut buf).unwrap();
    Ok((buf.to_vec(), seq_num))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        options::Options,
        util::rand_kv::{get_test_key, get_test_value},
    };

    use super::*;

    #[test]
    fn test_write_batch_one() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-1");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine.new_write_batch(WriteOptions::default()).unwrap();

        // 写入后未提交
        wb.put(get_test_key(1), get_test_value(11)).unwrap();
        wb.put(get_test_key(2), get_test_value(22)).unwrap();
        let get_res = engine.get(get_test_key(1));
        assert_eq!(get_res.err().unwrap(), Error::KeyNotFound);

        // 提交
        let commit_res = wb.commit();
        assert!(commit_res.is_ok());

        // 提交后读取
        let get_res = engine.get(get_test_key(1));
        assert_eq!(get_res.unwrap(), get_test_value(11));

        // 验证事务序列号
        assert_eq!(
            wb.engine.seq_num.load(std::sync::atomic::Ordering::SeqCst),
            2
        );

        std::fs::remove_dir_all(opts.dir_path.clone()).unwrap();
    }

    #[test]
    fn test_write_batch_two() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-2");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        let wb = engine.new_write_batch(WriteOptions::default()).unwrap();

        wb.put(get_test_key(1), get_test_value(11)).unwrap();
        wb.put(get_test_key(2), get_test_value(22)).unwrap();
        let commit_res = wb.commit();
        assert!(commit_res.is_ok());

        wb.put(get_test_key(3), get_test_value(33)).unwrap();
        wb.put(get_test_key(4), get_test_value(44)).unwrap();
        let commit_res = wb.commit();
        assert!(commit_res.is_ok());
        engine.close().unwrap();

        // 重新打开数据库
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        println!("{:#?}", engine.list_keys());
        // 读取数据
        let get_res = engine.get(get_test_key(1));
        assert_eq!(get_res.unwrap(), get_test_value(11));

        assert_eq!(
            wb.engine.seq_num.load(std::sync::atomic::Ordering::SeqCst),
            3
        );

        std::fs::remove_dir_all(opts.dir_path.clone()).unwrap();
    }

    #[test]
    fn test_write_batch_three() {
        let mut opts = Options::default();
        opts.dir_path = PathBuf::from("/tmp/bitcask-rs-batch-3");
        opts.data_file_size = 64 * 1024 * 1024;
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        println!("keys: {:?}", engine.list_keys());
        // let mut wb_opts = WriteOptions::default();
        // wb_opts.max_batch_size = 10000000;
        // let wb = engine.new_write_batch(wb_opts).unwrap();

        // for i in 0..=10000000 {
        //     wb.put(get_test_key(i), get_test_value(i)).unwrap();
        // }
        // let commit_res = wb.commit();
        // assert!(commit_res.is_ok());

        std::fs::remove_dir_all(opts.dir_path.clone()).unwrap();
    }
}
