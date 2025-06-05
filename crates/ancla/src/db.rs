// MIT License
//
// Copyright (c) 2024 Songlin Yang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use crate::bolt::{self, PAGE_HEADER_SIZE};
use crate::errors::DatabaseError;
use fnv_rs::{Fnv64, FnvHasher};
use std::cell::RefCell;
use std::ops::IndexMut;
use std::rc::Rc;
use std::sync::Arc;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read, Seek},
};

use typed_builder::TypedBuilder;

pub struct DB {
    file: File,

    page_datas: BTreeMap<bolt::Pgid, Arc<Page>>,
    meta0: Option<bolt::Meta>,
    meta1: Option<bolt::Meta>,
}

struct Page {
    id: u64,
    typ: PageType,
    overflow: u64,
    data: Vec<u8>,
    elem: Option<Element>,
}

enum Element {
    Branch(Vec<BranchElement>),
    Leaf(Vec<LeafElement>),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PageInfo {
    pub id: u64,
    pub typ: PageType,
    pub overflow: u64,
    pub capacity: u64,
    pub used: u64,
    pub parent_page_id: Option<u64>,
}

impl Ord for PageInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for PageInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone)]
pub struct Bucket {
    pub parent_bucket: Vec<u8>,
    pub page_id: u64,
    pub is_inline: bool,
    pub name: Vec<u8>,
    db: Rc<RefCell<DB>>,
}

impl Bucket {
    pub fn iter_buckets(&self) -> impl Iterator<Item = Bucket> {
        if self.is_inline {
            return BucketIterator {
                db: self.db.clone(),
                parent_bucket: Some(self.clone()),
                stack: Vec::new(),
            };
        }

        BucketIterator {
            db: self.db.clone(),
            parent_bucket: Some(self.clone()),
            stack: vec![IterItem {
                page_id: From::from(self.page_id),
                index: 0,
            }],
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PageType {
    Meta,
    DataLeaf,
    DataBranch,
    Freelist,
    Free,
}

#[derive(Debug, Clone)]
struct BranchElement {
    #[allow(dead_code)]
    key: Vec<u8>,
    pgid: u64,
}

#[derive(Debug, Clone)]
enum LeafElement {
    Bucket {
        name: Vec<u8>,
        pgid: u64,
    },
    #[allow(dead_code)]
    InlineBucket {
        name: Vec<u8>,
        items: Vec<KeyValue>,
    },
    KeyValue(KeyValue),
}

#[derive(Debug, Clone)]
pub struct KeyValue {
    #[allow(dead_code)]
    pub key: Vec<u8>,
    #[allow(dead_code)]
    pub value: Vec<u8>,
}

#[derive(Clone)]
pub enum DbItem {
    #[allow(dead_code)]
    KeyValue(KeyValue),
    #[allow(dead_code)]
    InlineBucket(Vec<u8>),
    #[allow(dead_code)]
    Bucket(Bucket),
}

#[allow(dead_code)]
struct DbItemIterator {
    db: Rc<RefCell<DB>>,
    stack: Vec<IterItem>,
}

impl Iterator for DbItemIterator {
    type Item = DbItem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.stack.is_empty() {
                return None;
            }

            let item = self.stack.index_mut(self.stack.len() - 1);
            let data = self.db.borrow_mut().read_page(item.page_id.into());
            match data.elem.as_ref().expect("must be leaf or branch") {
                Element::Leaf(leaf_elements) => {
                    if item.index < leaf_elements.len() {
                        let elem = leaf_elements[item.index].clone();
                        item.index += 1;
                        match elem {
                            LeafElement::Bucket { name, pgid } => {
                                self.stack.push(IterItem {
                                    page_id: From::from(pgid),
                                    index: 0,
                                });

                                return Some(DbItem::Bucket(Bucket {
                                    parent_bucket: Vec::new(),
                                    is_inline: false,
                                    page_id: pgid,
                                    name,
                                    db: self.db.clone(),
                                }));
                            }
                            LeafElement::InlineBucket { name, .. } => {
                                return Some(DbItem::InlineBucket(name));
                            }
                            LeafElement::KeyValue(kv) => {
                                return Some(DbItem::KeyValue(kv));
                            }
                        }
                    }

                    self.stack.pop();
                }
                Element::Branch(branch_elements) => {
                    if item.index < branch_elements.len() {
                        let elem = branch_elements[item.index].clone();
                        item.index += 1;
                        self.stack.push(IterItem {
                            page_id: From::from(elem.pgid),
                            index: 0,
                        });
                        continue;
                    }

                    self.stack.pop();
                }
            }
        }
    }
}

impl DB {
    fn read(&mut self, start: u64, size: usize) -> Vec<u8> {
        let mut data = vec![0u8; size];
        self.file.seek(io::SeekFrom::Start(start)).unwrap();
        let read_size = self.file.read(data.as_mut_slice()).unwrap();
        if read_size != size {
            panic!("read {} bytes, expected {}", read_size, size);
        }
        data
    }

    fn read_page(&mut self, page_id: u64) -> Arc<Page> {
        if let Some(data) = self.page_datas.get(&From::from(page_id)) {
            return Arc::clone(data);
        }

        let data = self.read(page_id * 4096, PAGE_HEADER_SIZE);
        let page: bolt::Page = TryFrom::try_from(data.as_slice()).unwrap();

        let data_len = 4096 * (page.overflow + 1) as usize;
        let data = self.read(page_id * 4096, data_len);

        let (typ, elem) = if page.flags.contains(bolt::PageFlag::LeafPageFlag) {
            (
                PageType::DataLeaf,
                Some(Element::Leaf(self.read_page_leaf_elements(&data))),
            )
        } else if page.flags.contains(bolt::PageFlag::BranchPageFlag) {
            (
                PageType::DataBranch,
                Some(Element::Branch(self.read_page_branch_elements(&data))),
            )
        } else if page.flags.contains(bolt::PageFlag::MetaPageFlag) {
            (PageType::Meta, None)
        } else if page.flags.contains(bolt::PageFlag::FreelistPageFlag) {
            (PageType::Freelist, None)
        } else {
            unreachable!("unknown type")
        };

        let data = Arc::new(Page {
            id: page_id,
            typ,
            overflow: page.overflow as u64,
            data,
            elem,
        });
        self.page_datas
            .insert(From::from(page_id), Arc::clone(&data));
        Arc::clone(&data)
    }

    fn read_page_branch_elements(&mut self, data: &[u8]) -> Vec<BranchElement> {
        let page: bolt::Page = TryFrom::try_from(data).unwrap();
        let mut branch_elements: Vec<BranchElement> = Vec::with_capacity(page.count as usize);
        for i in 0..page.count {
            let start = (16 + i * 16) as usize;
            let branch_element: bolt::BranchPageElement =
                bolt::BranchPageElement::try_from(data.get(start..data.len()).unwrap()).unwrap();
            let key_start = 16 + i * 16 + branch_element.pos as u16;
            let key_data = data
                .get((key_start as usize)..((key_start + branch_element.ksize as u16) as usize))
                .unwrap();
            branch_elements.push(BranchElement {
                key: key_data.to_vec(),
                pgid: branch_element.pgid.into(),
            });
        }
        branch_elements
    }

    fn read_page_leaf_elements(&mut self, data: &[u8]) -> Vec<LeafElement> {
        let page: bolt::Page = TryFrom::try_from(data).unwrap();
        let mut leaf_elements: Vec<LeafElement> = Vec::with_capacity(page.count as usize);
        for i in 0..page.count {
            let start = (16 + i * 16) as usize;
            let leaf_element: bolt::LeafPageElement =
                bolt::LeafPageElement::try_from(data.get(start..data.len()).unwrap()).unwrap();

            let key_start = 16 + i * 16 + (leaf_element.pos as u16);
            let key_end = key_start + (leaf_element.ksize as u16);
            let key = data.get((key_start as usize)..(key_end as usize)).unwrap();
            let value = data
                .get((key_end as usize)..((key_end + leaf_element.vsize as u16) as usize))
                .unwrap();
            if leaf_element.flags == 0x01 {
                let bucket_page_id = self.read_page_u64(value, 0);
                if bucket_page_id == 0 {
                    let page_leaf_elements = self.read_page_leaf_elements(value);
                    leaf_elements.push(LeafElement::InlineBucket {
                        name: key.to_vec(),
                        items: page_leaf_elements
                            .into_iter()
                            .map(|x| match x {
                                LeafElement::KeyValue(kv) => kv,
                                _ => panic!("unreachable"),
                            })
                            .collect(),
                    });
                } else {
                    leaf_elements.push(LeafElement::Bucket {
                        name: key.to_vec(),
                        pgid: bucket_page_id,
                    });
                }
            } else {
                leaf_elements.push(LeafElement::KeyValue(KeyValue {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }));
            }
        }
        leaf_elements
    }

    fn read_meta_page(&mut self, data: &[u8], id: u64) -> Result<bolt::Meta, DatabaseError> {
        let page: bolt::Page = TryFrom::try_from(data)?;
        if !page.flags.contains(bolt::PageFlag::MetaPageFlag) {
            return Err(DatabaseError::InvalidPageType {
                expect: bolt::PageFlag::MetaPageFlag.as_u16(),
                got: page.flags.as_u16(),
                id,
            });
        }

        let actual_checksum = u64::from_be_bytes(
            Fnv64::hash(&data[16..72])
                .as_bytes()
                .try_into()
                .expect("calculate checksum successfully"),
        );
        let meta: bolt::Meta = TryFrom::try_from(data)?;
        if meta.checksum != actual_checksum {
            return Err(DatabaseError::InvalidPageChecksum {
                expect: actual_checksum,
                got: meta.checksum,
                id,
            });
        }
        if meta.magic != bolt::MAGIC_NUMBER {
            return Err(DatabaseError::InvalidPageMagic {
                expect: bolt::MAGIC_NUMBER,
                got: meta.magic,
                id,
            });
        }
        if meta.version != bolt::DATAFILE_VERSION {
            return Err(DatabaseError::InvalidPageVersion {
                expect: bolt::DATAFILE_VERSION,
                got: meta.version,
                id,
            });
        }
        Ok(meta)
    }

    fn initialize(&mut self) -> Result<(), DatabaseError> {
        let data0 = self.read_page(0);
        let meta0 = self.read_meta_page(&data0.data, 0)?;
        self.meta0 = Some(meta0);

        let data1 = self.read_page(1);
        let meta1 = self.read_meta_page(&data1.data, 1)?;
        self.meta1 = Some(meta1);

        if self.meta0.is_none() && self.meta1.is_none() {
            return Err(DatabaseError::InvalidMeta);
        }

        Ok(())
    }

    fn get_meta(&mut self) -> bolt::Meta {
        if self.meta0.is_none() {
            return self.meta1.unwrap();
        }

        if self.meta1.is_none() {
            return self.meta0.unwrap();
        }

        let tx0 = self.meta0.unwrap().txid;
        let tx1 = self.meta1.unwrap().txid;
        if tx0 > tx1 {
            return self.meta0.unwrap();
        }

        self.meta1.unwrap()
    }

    fn read_page_u64(&mut self, page: &[u8], offset: u16) -> u64 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(offset as isize);
            let value_ptr = std::slice::from_raw_parts(offset_ptr, 8);
            u64::from_le_bytes(value_ptr.try_into().unwrap())
        }
    }

    fn read_freelist(&mut self, page: &[u8], count: u16) -> Vec<u64> {
        let mut freelist: Vec<u64> = Vec::with_capacity(count as usize);
        for i in 0..count {
            freelist.push(self.read_page_u64(page, i * 8 + 16));
        }
        freelist
    }

    pub fn open(ancla_options: AnclaOptions) -> Result<Rc<RefCell<DB>>, DatabaseError> {
        let file = File::open(ancla_options.db_path.clone()).map_err(|e| match e.kind() {
            io::ErrorKind::NotFound => DatabaseError::FileNotFound(ancla_options.db_path.clone()),
            _ => DatabaseError::IOError(ancla_options.db_path.clone(), e.to_string()),
        })?;

        let db = Rc::new(RefCell::new(DB {
            file,
            page_datas: BTreeMap::new(),
            meta0: None,
            meta1: None,
        }));

        db.borrow_mut().initialize()?;
        Ok(db)
    }

    pub fn iter_items(db: Rc<RefCell<DB>>) -> impl Iterator<Item = DbItem> {
        #[allow(unused_variables)]
        let meta = db.borrow_mut().get_meta();

        DbItemIterator {
            db: db.clone(),
            stack: vec![IterItem {
                page_id: meta.root_pgid,
                index: 0,
            }],
        }
    }

    pub fn iter_buckets(db: Rc<RefCell<DB>>) -> impl Iterator<Item = Bucket> {
        let meta = db.borrow_mut().get_meta();

        BucketIterator {
            db: db.clone(),
            parent_bucket: None,
            stack: vec![IterItem {
                page_id: meta.root_pgid,
                index: 0,
            }],
        }
    }

    pub fn iter_pages(db: Rc<RefCell<DB>>) -> impl Iterator<Item = PageInfo> {
        let meta = db.borrow_mut().get_meta();

        PageIterator {
            db: db.clone(),
            stack: vec![
                PageIterItem {
                    parent_page_id: None,
                    page_id: 0,
                    typ: PageType::Meta,
                },
                PageIterItem {
                    parent_page_id: None,
                    page_id: 1,
                    typ: PageType::Meta,
                },
                PageIterItem {
                    parent_page_id: None,
                    page_id: meta.freelist_pgid.into(),
                    typ: PageType::Freelist,
                },
                PageIterItem {
                    parent_page_id: None,
                    page_id: meta.root_pgid.into(),
                    typ: PageType::DataBranch,
                },
            ],
        }
    }

    pub fn info(db: Rc<RefCell<DB>>) -> Info {
        let meta = db.borrow_mut().get_meta();

        Info {
            page_size: meta.page_size,
        }
    }

    pub fn get_key_value(
        db: Rc<RefCell<DB>>,
        buckets: &[String],
        key: &String,
    ) -> Option<KeyValue> {
        let meta = db.borrow_mut().get_meta();
        db.borrow_mut()
            .get_key_value_inner(buckets, key, meta.root_pgid.into())
    }

    fn get_key_value_inner(
        &mut self,
        buckets: &[String],
        key: &String,
        pgid: u64,
    ) -> Option<KeyValue> {
        let data = self.read_page(pgid);

        match data.elem.as_ref()? {
            Element::Branch(branch_elements) => {
                let r = branch_elements
                    .binary_search_by_key(&key.as_bytes(), |elem| elem.key.as_slice());
                let index = r.unwrap_or_else(|idx| if idx > 0 { idx - 1 } else { 0 });
                self.get_key_value_inner(buckets, key, branch_elements[index].pgid)
            }
            Element::Leaf(leaf_elements) => {
                for leaf_item in leaf_elements {
                    match leaf_item {
                        LeafElement::KeyValue(kv) => {
                            if kv.key == key.as_bytes() && buckets.is_empty() {
                                return Some(kv.clone());
                            }
                        }
                        LeafElement::Bucket { name, pgid } => {
                            if buckets.is_empty() {
                                continue;
                            }

                            if name == buckets[0].as_bytes() {
                                return self.get_key_value_inner(&buckets[1..], key, *pgid);
                            }
                        }
                        LeafElement::InlineBucket { name, items } => {
                            if buckets.len() != 1 {
                                continue;
                            }

                            if name == buckets[0].as_bytes() {
                                for item in items {
                                    if item.key == key.as_bytes() {
                                        return Some(item.clone());
                                    }
                                }
                            }
                        }
                    }
                }
                None
            }
        }
    }
}

pub struct Info {
    pub page_size: u32,
}

struct PageIterator {
    db: Rc<RefCell<DB>>,
    stack: Vec<PageIterItem>,
}

struct PageIterItem {
    parent_page_id: Option<u64>,
    page_id: u64,
    typ: PageType,
}

impl Iterator for PageIterator {
    type Item = PageInfo;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.is_empty() {
            return None;
        }

        let item = self.stack.remove(0);
        if item.typ == PageType::Free {
            return Some(PageInfo {
                id: item.page_id,
                typ: PageType::Free,
                overflow: 0,
                capacity: 4096,
                used: 0,
                parent_page_id: None,
            });
        }

        let data = self.db.borrow_mut().read_page(item.page_id);
        if data.typ == PageType::Meta {
            return Some(PageInfo {
                id: data.id,
                typ: PageType::Meta,
                overflow: data.overflow,
                capacity: 4096,
                used: 80,
                parent_page_id: None,
            });
        } else if data.typ == PageType::Freelist {
            let page: bolt::Page = TryFrom::try_from(data.data.as_slice()).unwrap();
            let freelist = self.db.borrow_mut().read_freelist(&data.data, page.count);
            for &i in &freelist {
                // See
                // 1. https://stackoverflow.com/questions/59123462/why-is-iterating-over-a-collection-via-for-loop-considered-a-move-in-rust
                // 2. https://doc.rust-lang.org/reference/expressions/loop-expr.html#iterator-loops
                self.stack.push(PageIterItem {
                    parent_page_id: None,
                    page_id: i,
                    typ: PageType::Free,
                });
            }

            return Some(PageInfo {
                id: item.page_id,
                typ: PageType::Freelist,
                overflow: page.overflow as u64,
                capacity: 4096,
                used: 16 + (page.count as u64 * 8),
                parent_page_id: None,
            });
        }

        let page: bolt::Page = TryFrom::try_from(data.data.as_slice()).unwrap();
        match data.elem.as_ref().expect("must be leaf or branch") {
            Element::Branch(branch_elements) => {
                for branch_item in branch_elements {
                    self.stack.push(PageIterItem {
                        parent_page_id: Some(item.page_id),
                        page_id: branch_item.pgid,
                        typ: PageType::DataBranch,
                    });
                }

                Some(PageInfo {
                    id: item.page_id,
                    typ: PageType::DataBranch,
                    overflow: data.overflow,
                    capacity: 4096,
                    used: 16 + (page.count as u64 * 12),
                    parent_page_id: item.parent_page_id,
                })
            }
            Element::Leaf(leaf_elements) => {
                for leaf_item in leaf_elements {
                    if let LeafElement::Bucket {
                        name: _,
                        pgid: pg_id,
                    } = leaf_item
                    {
                        self.stack.push(PageIterItem {
                            parent_page_id: Some(item.page_id),
                            page_id: *pg_id,
                            typ: PageType::DataLeaf,
                        });
                    }
                }

                Some(PageInfo {
                    id: item.page_id,
                    typ: PageType::DataLeaf,
                    overflow: page.overflow as u64,
                    capacity: 4096,
                    used: 16 + (page.count as u64 * 12),
                    parent_page_id: item.parent_page_id,
                })
            }
        }
    }
}

struct BucketIterator {
    db: Rc<RefCell<DB>>,
    parent_bucket: Option<Bucket>,
    stack: Vec<IterItem>,
}

struct IterItem {
    page_id: bolt::Pgid,
    index: usize,
}

impl Iterator for BucketIterator {
    type Item = Bucket;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.stack.is_empty() {
                return None;
            }

            let item = self.stack.index_mut(self.stack.len() - 1);
            let data = self.db.borrow_mut().read_page(item.page_id.into());

            match data.elem.as_ref().expect("must be leaf or branch") {
                Element::Branch(branch_elements) => {
                    if item.index < branch_elements.len() {
                        let elem = branch_elements[item.index].clone();
                        item.index += 1;
                        self.stack.push(IterItem {
                            page_id: From::from(elem.pgid),
                            index: 0,
                        });
                        continue;
                    }

                    self.stack.pop();
                }
                Element::Leaf(leaf_elements) => {
                    if item.index < leaf_elements.len() {
                        let elem = leaf_elements[item.index].clone();
                        item.index += 1;
                        match elem {
                            LeafElement::Bucket { name, pgid } => {
                                return Some(Bucket {
                                    parent_bucket: self
                                        .parent_bucket
                                        .as_ref()
                                        .map_or_else(Vec::new, |bucket| bucket.name.clone()),
                                    is_inline: false,
                                    page_id: pgid,
                                    name,
                                    db: self.db.clone(),
                                });
                            }
                            LeafElement::InlineBucket { name, items: _ } => {
                                return Some(Bucket {
                                    parent_bucket: self
                                        .parent_bucket
                                        .as_ref()
                                        .map_or_else(Vec::new, |bucket| bucket.name.clone()),
                                    is_inline: true,
                                    page_id: 0,
                                    name,
                                    db: self.db.clone(),
                                });
                            }
                            LeafElement::KeyValue(_) => {}
                        }
                        continue;
                    }

                    self.stack.pop();
                }
            }
        }
    }
}

// bucket -- list all bucket
// check -- is page double free、is all page reachable
// compact --
// dump -- print pages
// page-item -- print page items
// get -- print key value
// info -- print page size  -> todo
// keys -- print keys -> todo
// page -- print pages
// stats -- ....
// surgery --
// print etcd's interval data

#[derive(TypedBuilder)]
pub struct AnclaOptions {
    db_path: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde::Serialize;
    use std::fs;
    use std::path::Path;

    #[derive(Serialize, Deserialize, Debug)]
    #[serde(deny_unknown_fields)]
    struct Bucket {
        #[serde(rename = "Name")]
        name: String,
        #[serde(rename = "Buckets")]
        #[serde(default)]
        buckets: Vec<Bucket>,
        #[serde(rename = "Items")]
        #[serde(default)]
        items: Vec<Item>,
    }

    #[derive(Serialize, Deserialize, Debug)]
    #[serde(rename_all = "PascalCase")]
    struct Item {
        key: String,
        value: String,
    }

    fn assert_buckets_equal(
        parent: String,
        actual_buckets: &[crate::db::Bucket],
        expect_buckets: &[Bucket],
    ) {
        assert_eq!(
            actual_buckets.len(),
            expect_buckets.len(),
            "different child buckets num under: {}",
            parent
        );

        for (actual, expect) in actual_buckets.iter().zip(expect_buckets.iter()) {
            assert_eq!(
                actual.name,
                expect.name.clone().into_bytes(),
                "different child bucket name under: {}",
                parent
            );
            let actual_child_buckets = actual.iter_buckets().collect::<Vec<_>>();
            assert_buckets_equal(
                format!("{}/{}", parent, expect.name),
                &actual_child_buckets,
                &expect.buckets,
            );
        }
    }

    #[test]
    fn test_iter_buckets() {
        let root_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        let db = DB::open(
            AnclaOptions::builder()
                .db_path(
                    root_dir
                        .join("testdata")
                        .join("data.db")
                        .to_str()
                        .unwrap()
                        .to_string(),
                )
                .build(),
        )
        .expect("open db successfully");
        let actual_buckets = DB::iter_buckets(db.clone()).collect::<Vec<_>>();

        let content =
            fs::read_to_string(format!("{}/testdata/data.json", root_dir.to_str().unwrap()))
                .expect("Unable to read file");
        let expect_buckets: Vec<Bucket> = serde_json::from_str(&content).unwrap();

        assert_buckets_equal(String::from(""), &actual_buckets, &expect_buckets);
    }
}
