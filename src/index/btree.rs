use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::data::log_record::LogRecordPos;
use crate::options::IteratorOptions;

use super::{IndexIterator, Indexer};

/// BTree索引，封装了标准库的BTreeMap
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        let remove_res = write_guard.remove(&key);
        remove_res.is_some()
    }

    fn iter(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let read_guard = self.tree.read();
        // 将所有key和pos信息从BTree索引取出来(内存会不够吗)
        let mut items = read_guard
            .iter()
            .map(|(key, pos)| (key.clone(), pos.clone()))
            .collect::<Vec<_>>();
        if options.reverse {
            items.reverse();
        }
        Box::new(BTreeIterator {
            items,
            curr_index: 0,
            options,
        })
    }

    fn list_keys(&self) -> crate::error::Result<Vec<Bytes>> {
        let read_guard = self.tree.read();
        let keys = read_guard
            .iter()
            .map(|(key, _)| Bytes::copy_from_slice(key))
            .collect::<Vec<_>>();
        Ok(keys)
    }
}

/// BTree索引迭代器
pub struct BTreeIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>,
    curr_index: usize,
    options: IteratorOptions,
}

impl IndexIterator for BTreeIterator {
    fn rewind(&mut self) {
        self.curr_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        self.curr_index = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(equal_idx) => equal_idx,
            Err(insert_idx) => insert_idx,
        };
    }

    fn next(&mut self) -> Option<(&[u8], &LogRecordPos)> {
        if self.curr_index >= self.items.len() {
            return None;
        }
        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;
            if prefix.is_empty() || item.0.starts_with(prefix) {
                return Some((&item.0, &item.1));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_put() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert_eq!(res1, true);

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
            },
        );
        assert_eq!(res2, true);
    }

    #[test]
    fn test_btree_get() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert_eq!(res1, true);
        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
            },
        );
        assert_eq!(res2, true);

        let pos1 = bt.get("".as_bytes().to_vec());
        assert!(pos1.is_some());
        assert_eq!(pos1.unwrap().file_id, 1);

        let pos2 = bt.get("aa".as_bytes().to_vec());
        assert!(pos2.is_some());
        assert_eq!(pos2.unwrap().file_id, 11);
    }

    #[test]
    fn test_btree_delete() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert_eq!(res1, true);
        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
            },
        );
        assert_eq!(res2, true);

        let del1 = bt.delete("".as_bytes().to_vec());
        assert!(del1);

        let del2 = bt.delete("aa".as_bytes().to_vec());
        assert!(del2);

        let del3 = bt.delete("not exist".as_bytes().to_vec());
        assert!(!del3);
    }

    #[test]
    fn test_btree_iterator_seek() {
        let bt = BTree::new();
        // 没有数据的情况
        let mut iter = bt.iter(IteratorOptions::default());
        iter.seek("aa".into());

        let res = iter.next();
        assert!(res.is_none());
        // 有一条记录的情况
        bt.put(
            "ccde".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter = bt.iter(IteratorOptions::default());
        iter.seek("aa".into());
        let res = iter.next();
        assert!(res.is_some());

        let mut iter = bt.iter(IteratorOptions::default());
        iter.seek("zz".into());
        let res = iter.next();
        assert!(res.is_none());

        // 多条记录
        bt.put(
            "bbed".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "aaed".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "cadd".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter = bt.iter(IteratorOptions::default());
        iter.seek("b".into());
        while let Some(item) = iter.next() {
            // Ok("bbed") Ok("cadd") Ok("ccde")
            assert!(item.0.len() > 0);
        }

        let mut iter = bt.iter(IteratorOptions::default());
        iter.seek("cadd".into());
        while let Some(item) = iter.next() {
            // Ok("cadd") Ok("ccde")
            assert!(item.0.len() > 0);
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
        }

        let mut iter = bt.iter(IteratorOptions::default());
        iter.seek("zzz".into());
        let res = iter.next();
        assert!(res.is_none());

        // 反向迭代
        let mut opts = IteratorOptions::default();
        opts.reverse = true;

        let mut iter = bt.iter(opts);
        iter.seek("bb".into());
        while let Some(item) = iter.next() {
            // Ok("aaed")
            assert!(item.0.len() > 0);
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
        }
    }

    #[test]
    fn test_btree_iterator_next() {
        let bt = BTree::new();
        // 没有数据的情况
        let mut iter = bt.iter(IteratorOptions::default());
        // println!("{:?}", iter.next());
        assert!(iter.next().is_none());

        // 有一条记录的情况
        bt.put(
            "cadd".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut opts = IteratorOptions::default();
        opts.reverse = true;
        let mut iter = bt.iter(opts.clone());
        assert!(iter.next().is_some());

        // 有多条记录的情况
        bt.put(
            "bbed".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "aaed".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            "cdea".into(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter = bt.iter(opts.clone());
        while let Some(item) = iter.next() {
            // Ok("cdea")
            // Ok("cadd")
            // Ok("bbed")
            // Ok("aaed")
            assert!(item.0.len() > 0);
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
        }

        // 有前缀的情况
        opts.prefix = "c".into();
        opts.reverse = false;
        let mut iter = bt.iter(opts.clone());
        while let Some(item) = iter.next() {
            // Ok("cadd")
            // Ok("cdea")
            assert!(item.0.len() > 0);
            // println!("{:?}", String::from_utf8(item.0.to_vec()));
        }
    }
}
