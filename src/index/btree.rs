use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::data::log_record::LogRecordPos;
use crate::options::IteratorOptions;

use super::{IndexInterator, Indexer};

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

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexInterator> {
        let mut items = self
            .tree
            .read()
            .iter()
            .map(|(key, pos)| (key.clone(), pos.clone()))
            .collect::<Vec<_>>();
        if options.reverse {
            items.reverse()
        }
        Box::new(BTreeIterator {
            items,
            curr_idx: 0,
            options,
        })
    }
}

/// BTree索引的迭代器
pub struct BTreeIterator {
    /// key + pos
    items: Vec<(Vec<u8>, LogRecordPos)>,

    /// 当前索引
    curr_idx: usize,

    /// 迭代器选项
    options: IteratorOptions,
}

impl IndexInterator for BTreeIterator {
    fn rewind(&mut self) {
        self.curr_idx = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        self.curr_idx = match self.items.binary_search_by(|(k, _)| {
            if self.options.reverse {
                k.cmp(&key).reverse()
            } else {
                k.cmp(&key)
            }
        }) {
            Ok(position) => position,
            Err(insert_position) => insert_position,
        };
    }

    fn next(&mut self) -> Option<(&[u8], &LogRecordPos)> {
        if self.curr_idx >= self.items.len() {
            return None;
        }
        while let Some((key, pos)) = self.items.get(self.curr_idx) {
            self.curr_idx += 1;
            if self.options.prefix.is_empty() || key.starts_with(&self.options.prefix) {
                return Some((key, pos));
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
        // 没有数据
        let mut iter = bt.iterator(IteratorOptions::default());

        iter.seek(b"aa".to_vec());
        let res = iter.next();
        assert!(res.is_none());

        // 有一条数据
        bt.put(
            b"bbb".to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter = bt.iterator(IteratorOptions::default());
        iter.seek(b"a".to_vec());
        let res = iter.next();
        assert!(res.is_some());
        let mut iter = bt.iterator(IteratorOptions::default());
        iter.seek(b"cc".to_vec());
        let res = iter.next();
        assert!(res.is_none());

        // 有多个数据
        bt.put(
            b"aad".to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        bt.put(
            b"bbed".to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 30,
            },
        );
        bt.put(
            b"cadd".to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 40,
            },
        );
        let mut iter = bt.iterator(IteratorOptions::default());
        iter.seek(b"cad".to_vec());
        // while let Some((key, pos)) = iter.next() {
        //     println!("key: {:?}, pos: {:?}", String::from_utf8_lossy(key), pos);
        // }
        iter.seek(b"xxx".to_vec());
        // while let Some((key, pos)) = iter.next() {
        //     println!("key: {:?}, pos: {:?}", String::from_utf8_lossy(key), pos);
        // }
        iter.seek(b"".to_vec());
        // while let Some((key, pos)) = iter.next() {
        //     println!("key: {:?}, pos: {:?}", String::from_utf8_lossy(key), pos);
        // }

        // 反向迭代
        let mut options = IteratorOptions::default();
        options.reverse = true;
        let mut iter = bt.iterator(options);
        iter.seek(b"bbb".to_vec());
        // while let Some((key, pos)) = iter.next() {
        //     println!("key: {:?}, pos: {:?}", String::from_utf8_lossy(key), pos);
        // }
    }

    #[test]
    fn test_btree_iterator_next() {
        let bt = BTree::new();
        let mut iter = bt.iterator(IteratorOptions::default());
        assert!(iter.next().is_none());
        // 有一条数据，反向迭代
        let mut options = IteratorOptions::default();
        options.reverse = true;
        bt.put(
            b"aaa".to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        let mut iter = bt.iterator(options);
        assert!(iter.next().is_some());
        // 有两条数据，反向
        let mut options = IteratorOptions::default();
        options.reverse = true;
        bt.put(
            b"bbb".to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 20,
            },
        );
        bt.put(
            b"ccc".to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 20,
            },
        );
        let mut iter = bt.iterator(options);

        while let Some((key, pos)) = iter.next() {
            println!("key: {:?}, pos: {:?}", String::from_utf8_lossy(key), pos);
        }
    }
}
