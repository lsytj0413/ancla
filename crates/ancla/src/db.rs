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

use crate::errors::DatabaseError;
use boltypes as bolt;
use serde::{Deserialize, Serialize};
use std::ops::IndexMut;
use std::sync::{Arc, Mutex};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read, Seek},
};

use typed_builder::TypedBuilder;

/// DB is the bolt reader for multi thread.
#[derive(Clone, Debug)]
pub struct DB {
    inner: Arc<Mutex<DBInner>>,
}

impl DB {
    /// Attempts to open bolt file in read-only mode.
    ///
    /// # Errors
    ///
    /// This function will return an error if file doesn't already exist,
    /// other errors may also be returned according to bolt.
    pub fn open(ancla_options: AnclaOptions) -> Result<Self, DatabaseError> {
        let file = File::open(ancla_options.db_path.clone()).map_err(|e| match e.kind() {
            io::ErrorKind::NotFound => DatabaseError::FileNotFound(ancla_options.db_path.clone()),
            _ => DatabaseError::IOError(ancla_options.db_path.clone(), e.to_string()),
        })?;

        let mut db = DBInner {
            file,
            page_datas: BTreeMap::new(),
            meta0: None,
            meta1: None,
            page_size: 0,
        };

        if let Some(page_size) = ancla_options.page_size {
            db.page_size = page_size;
        } else {
            // Determine page_size before full initialization
            db.page_size = db.determine_page_size()?;
        }

        db.initialize()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(db)),
        })
    }

    /// Creates an bucket iterator, and the iterator will return errors when
    /// read database.
    pub fn iter_buckets(&self) -> impl Iterator<Item = Result<Bucket, DatabaseError>> {
        BucketIterator {
            iter: self.iter_items(),
        }
    }

    /// Creates an item iterator (contains bucket、key-value and so on), and
    /// the iterator will return errors when read database.
    pub fn iter_items(&self) -> impl Iterator<Item = Result<DbItem, DatabaseError>> {
        let (meta, _) = self.inner.lock().unwrap().get_meta();

        DbItemIterator {
            db: self.clone(),
            stack: vec![IterItem {
                node: ItemNode::Page(meta.root_pgid),
                index: 0,
                depth: None,
                parent_bucket: None,
            }],
        }
    }

    /// Creates an page iterator, and the iterator will return errors when
    /// read database.
    pub fn iter_pages(&self) -> impl Iterator<Item = Result<PageInfo, DatabaseError>> {
        let (meta, _) = self.inner.lock().unwrap().get_meta();

        PageIterator {
            db: self.clone(),
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

    pub fn info(&self) -> Info {
        let (meta, pgid) = self.inner.lock().unwrap().get_meta();

        Info {
            page_size: meta.page_size,
            max_pgid: meta.max_pgid,
            root_pgid: meta.root_pgid,
            freelist_pgid: meta.freelist_pgid,
            txid: meta.txid,
            meta_pgid: pgid,
        }
    }

    pub fn get_key_value(&self, buckets: &[String], key: &String) -> Option<KeyValue> {
        let (meta, _) = self.inner.lock().unwrap().get_meta();
        self.inner
            .lock()
            .unwrap()
            .get_key_value_inner(buckets, key, meta.root_pgid.into())
            .ok()?
    }
}

pub struct DBInner {
    file: File,

    page_datas: BTreeMap<boltypes::Pgid, Arc<Page>>,
    meta0: Option<boltypes::Meta>,
    meta1: Option<boltypes::Meta>,
    page_size: u32,
}

impl std::fmt::Debug for DBInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DB")
            .field("page_datas", &self.page_datas)
            .field("meta0", &self.meta0)
            .field("meta1", &self.meta1)
            .field("page_size", &self.page_size)
            .finish()
    }
}

struct Page {
    id: u64,
    typ: PageType,
    overflow: u64,
    data: boltypes::Page,
    elem: Option<Element>,
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("id", &self.id)
            .field("typ", &self.typ)
            .field("overflow", &self.overflow)
            .field("elem", &self.elem)
            .finish()
    }
}

#[derive(Debug)]
enum Element {
    Branch(Vec<boltypes::BranchElement>),
    Leaf(Vec<boltypes::LeafElement>),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct PageInfo {
    pub id: u64,
    pub typ: PageType,
    pub overflow: u64,
    pub capacity: u64,
    pub used: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
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
    pub depth: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum PageType {
    Meta,
    DataLeaf,
    DataBranch,
    Freelist,
    Free,
}

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub depth: u64,
}

#[derive(Clone)]
pub enum DbItem {
    KeyValue(KeyValue),
    InlineBucket(Bucket),
    Bucket(Bucket),
}

struct DbItemIterator {
    db: DB,
    stack: Vec<IterItem>,
}

impl Iterator for DbItemIterator {
    type Item = Result<DbItem, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.stack.is_empty() {
                return None;
            }

            let item = self.stack.index_mut(self.stack.len() - 1);
            let data = match item.node {
                ItemNode::Page(page_id) => {
                    match self.db.inner.lock().unwrap().read_page(page_id.into()) {
                        Ok(d) => d,
                        Err(e) => return Some(Err(e)),
                    }
                }
                ItemNode::Elements(ref kvs) => {
                    if item.index < kvs.len() {
                        item.index += 1;
                        let kv = &kvs[item.index - 1];
                        return Some(Ok(DbItem::KeyValue(KeyValue {
                            key: kv.key.clone(),
                            value: kv.value.clone(),
                            depth: item.depth.map(|depth| depth + 1).unwrap_or(0),
                        })));
                    }

                    self.stack.pop();
                    continue;
                }
            };
            let parent_bucket = item.parent_bucket.clone();

            match data.elem.as_ref().expect("must be leaf or branch") {
                Element::Leaf(leaf_elements) => {
                    let depth = item.depth.map(|depth| depth + 1).unwrap_or(0);
                    if item.index < leaf_elements.len() {
                        let elem = leaf_elements[item.index].clone();
                        item.index += 1;
                        match elem {
                            boltypes::LeafElement::Bucket { name, pgid } => {
                                self.stack.push(IterItem {
                                    node: ItemNode::Page(pgid),
                                    index: 0,
                                    depth: Some(depth),
                                    parent_bucket: parent_bucket.clone(),
                                });

                                return Some(Ok(DbItem::Bucket(Bucket {
                                    parent_bucket: parent_bucket
                                        .clone()
                                        .as_ref()
                                        .map_or_else(Vec::new, |bucket| bucket.name.clone()),
                                    is_inline: false,
                                    page_id: Into::<u64>::into(pgid),
                                    name,
                                    depth,
                                })));
                            }
                            boltypes::LeafElement::InlineBucket { name, pgid, items } => {
                                self.stack.push(IterItem {
                                    parent_bucket: parent_bucket.clone(),
                                    node: ItemNode::Elements(
                                        items
                                            .iter()
                                            .map(|x| KeyValue {
                                                key: x.key.clone(),
                                                value: x.value.clone(),
                                                depth: 0,
                                            })
                                            .collect(),
                                    ),
                                    index: 0,
                                    depth: Some(depth),
                                });
                                return Some(Ok(DbItem::InlineBucket(Bucket {
                                    parent_bucket: parent_bucket
                                        .clone()
                                        .as_ref()
                                        .map_or_else(Vec::new, |bucket| bucket.name.clone()),
                                    is_inline: true,
                                    page_id: Into::<u64>::into(pgid),
                                    name,
                                    depth,
                                })));
                            }
                            boltypes::LeafElement::KeyValue(kv) => {
                                return Some(Ok(DbItem::KeyValue(KeyValue {
                                    key: kv.key,
                                    value: kv.value,
                                    depth,
                                })));
                            }
                        }
                    }

                    self.stack.pop();
                }
                Element::Branch(branch_elements) => {
                    if item.index < branch_elements.len() {
                        let elem = branch_elements[item.index].clone();
                        item.index += 1;
                        let depth = item.depth;
                        self.stack.push(IterItem {
                            node: ItemNode::Page(elem.pgid),
                            index: 0,
                            depth,
                            parent_bucket: parent_bucket.clone(),
                        });
                        continue;
                    }

                    self.stack.pop();
                }
            }
        }
    }
}

impl DBInner {
    fn read(&mut self, start: u64, size: usize) -> Vec<u8> {
        let mut data = vec![0u8; size];
        self.file.seek(io::SeekFrom::Start(start)).unwrap();
        let read_size = self.file.read(data.as_mut_slice()).unwrap();
        if read_size != size {
            panic!("read {read_size} bytes, expected {size}");
        }
        data
    }

    fn read_page(&mut self, page_id: u64) -> Result<Arc<Page>, DatabaseError> {
        if let Some(data) = self.page_datas.get(&From::from(page_id)) {
            return Ok(Arc::clone(data));
        }

        let data = self.read(page_id * self.page_size as u64, boltypes::PAGE_HEADER_SIZE);
        let page: boltypes::PageHeader =
            TryFrom::try_from(data.as_slice()).map_err(DatabaseError::BoltTypes)?;

        let data_len = self.page_size as usize * (page.overflow + 1) as usize;
        let data = self.read(page_id * self.page_size as u64, data_len);
        let page_data =
            bolt::Page::new(data, self.page_size as usize).map_err(DatabaseError::BoltTypes)?;

        let (typ, elem) = match &page_data {
            boltypes::Page::MetaPage(_) => (PageType::Meta, None),
            boltypes::Page::FreelistPage(_) => (PageType::Freelist, None),
            boltypes::Page::LeafPage(leaf) => (
                PageType::DataLeaf,
                Some(Element::Leaf(
                    leaf.leaf_elements().map_err(DatabaseError::BoltTypes)?,
                )),
            ),
            boltypes::Page::BranchPage(branch) => (
                PageType::DataBranch,
                Some(Element::Branch(
                    branch.branch_elements().map_err(DatabaseError::BoltTypes)?,
                )),
            ),
        };

        let data = Arc::new(Page {
            id: page_id,
            typ,
            overflow: page.overflow as u64,
            data: page_data,
            elem,
        });
        self.page_datas
            .insert(From::from(page_id), Arc::clone(&data));
        Ok(Arc::clone(&data))
    }

    // TODO: remove unwrap
    fn initialize(&mut self) -> Result<(), DatabaseError> {
        let data0 = self.read_page(0)?;
        let meta0 = match &data0.data {
            boltypes::Page::MetaPage(meta) => meta.meta().map_err(DatabaseError::BoltTypes)?,
            _ => unreachable!("wrong type of page 0"),
        };
        self.meta0 = Some(meta0);

        let data1 = self.read_page(1)?;
        let meta1 = match &data1.data {
            boltypes::Page::MetaPage(meta) => meta.meta().map_err(DatabaseError::BoltTypes)?,
            _ => unreachable!("wrong type of page 1"),
        };
        self.meta1 = Some(meta1);

        if self.meta0.is_none() && self.meta1.is_none() {
            return Err(DatabaseError::InvalidMeta);
        }

        Ok(())
    }

    fn get_meta(&mut self) -> (bolt::Meta, bolt::Pgid) {
        if self.meta0.is_none() {
            return (self.meta1.unwrap(), 1.into());
        }

        if self.meta1.is_none() {
            return (self.meta0.unwrap(), 0.into());
        }

        let tx0 = self.meta0.unwrap().txid;
        let tx1 = self.meta1.unwrap().txid;
        if tx0 > tx1 {
            return (self.meta0.unwrap(), 0.into());
        }

        (self.meta1.unwrap(), 1.into())
    }

    fn determine_page_size(&mut self) -> Result<u32, DatabaseError> {
        // Phase 1: Attempt to read and validate meta0 (fixed position)
        const META_READ_LEN: usize =
            boltypes::PAGE_HEADER_SIZE + std::mem::size_of::<boltypes::Meta>();
        let mut buf_meta0 = vec![0; META_READ_LEN];

        // Attempt to read meta0 from the beginning of the file
        self.file
            .seek(io::SeekFrom::Start(0))
            .map_err(|e| DatabaseError::Io(Arc::new(e)))?;
        let read_bytes_meta0 = self
            .file
            .read(&mut buf_meta0)
            .map_err(|e| DatabaseError::Io(Arc::new(e)))?;

        if read_bytes_meta0 >= META_READ_LEN {
            if let Ok(valid_meta) = boltypes::Meta::try_from(buf_meta0.as_slice()) {
                return Ok(valid_meta.page_size);
            }
        }

        // Phase 2: If meta0 is invalid, search the entire file for a valid metadata page
        self.search_for_meta_page()
    }

    fn search_for_meta_page(&mut self) -> Result<u32, DatabaseError> {
        const META_READ_LEN: usize =
            boltypes::PAGE_HEADER_SIZE + std::mem::size_of::<boltypes::Meta>();
        const MAX_PAGE_SIZE: u32 = 1024 * 1024; // 1MB

        // Iterate through powers of 2 for page_size, from 512 to 1MB
        for page_size_candidate in (9..=20).map(|i| 1 << i) {
            if !(512..=MAX_PAGE_SIZE).contains(&page_size_candidate) {
                continue;
            }

            let meta_page_offset = page_size_candidate as u64;
            let mut buf = vec![0; META_READ_LEN];

            // Attempt to read the second meta page (meta1)
            self.file
                .seek(io::SeekFrom::Start(meta_page_offset))
                .map_err(|e| DatabaseError::Io(Arc::new(e)))?;
            let read_bytes = self
                .file
                .read(&mut buf)
                .map_err(|e| DatabaseError::Io(Arc::new(e)))?;

            if read_bytes >= META_READ_LEN {
                if let Ok(valid_meta) = boltypes::Meta::try_from(buf.as_slice()) {
                    // Validate that the page_size in the meta matches our candidate
                    if valid_meta.page_size == page_size_candidate {
                        return Ok(valid_meta.page_size);
                    }
                }
            }
        }

        Err(DatabaseError::InvalidMeta)
    }

    fn get_key_value_inner(
        &mut self,
        buckets: &[String],
        key: &String,
        pgid: u64,
    ) -> Result<Option<KeyValue>, DatabaseError> {
        let data = self.read_page(pgid)?;

        match data.elem.as_ref() {
            None => Ok(None),
            Some(Element::Branch(branch_elements)) => {
                let r = branch_elements
                    .binary_search_by_key(&key.as_bytes(), |elem| elem.key.as_slice());
                let index = r.unwrap_or_else(|idx| if idx > 0 { idx - 1 } else { 0 });
                self.get_key_value_inner(buckets, key, branch_elements[index].pgid.into())
            }
            Some(Element::Leaf(leaf_elements)) => {
                for leaf_item in leaf_elements {
                    match leaf_item {
                        boltypes::LeafElement::KeyValue(kv) => {
                            if kv.key == key.as_bytes() && buckets.is_empty() {
                                return Ok(Some(KeyValue {
                                    key: kv.key.clone(),
                                    value: kv.value.clone(),
                                    depth: 0,
                                }));
                            }
                        }
                        boltypes::LeafElement::Bucket { name, pgid } => {
                            if buckets.is_empty() {
                                continue;
                            }

                            if name == buckets[0].as_bytes() {
                                return self.get_key_value_inner(
                                    &buckets[1..],
                                    key,
                                    Into::<u64>::into(*pgid),
                                );
                            }
                        }
                        boltypes::LeafElement::InlineBucket { name, items, .. } => {
                            if buckets.len() != 1 {
                                continue;
                            }

                            if name == buckets[0].as_bytes() {
                                for item in items {
                                    if item.key == key.as_bytes() {
                                        return Ok(Some(KeyValue {
                                            key: item.key.clone(),
                                            value: item.value.clone(),
                                            depth: 0,
                                        }));
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Info {
    pub page_size: u32,
    pub max_pgid: bolt::Pgid,
    pub root_pgid: bolt::Pgid,
    pub freelist_pgid: bolt::Pgid,
    pub txid: u64,
    pub meta_pgid: bolt::Pgid,
}

struct PageIterator {
    db: DB,
    stack: Vec<PageIterItem>,
}

struct PageIterItem {
    parent_page_id: Option<u64>,
    page_id: u64,
    typ: PageType,
}

impl Iterator for PageIterator {
    type Item = Result<PageInfo, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stack.is_empty() {
            return None;
        }

        let item = self.stack.remove(0);
        if item.typ == PageType::Free {
            return Some(Ok(PageInfo {
                id: item.page_id,
                typ: PageType::Free,
                overflow: 0,
                capacity: 4096,
                used: 0,
                parent_page_id: None,
            }));
        }

        let data = match self.db.inner.lock().unwrap().read_page(item.page_id) {
            Ok(d) => d,
            Err(e) => return Some(Err(e)),
        };

        if data.typ == PageType::Meta {
            let capacity = data.data.capacity();
            let used = data.data.used();
            return Some(Ok(PageInfo {
                id: data.id,
                typ: PageType::Meta,
                overflow: data.overflow,
                capacity: capacity as u64,
                used: used as u64,
                parent_page_id: None,
            }));
        } else if data.typ == PageType::Freelist {
            let page: bolt::PageHeader = match TryFrom::try_from(data.data.as_slice()) {
                Ok(p) => p,
                Err(e) => return Some(Err(DatabaseError::BoltTypes(e))),
            };
            let freelist = match &data.data {
                boltypes::Page::FreelistPage(freelist) => match freelist.free_pages() {
                    Ok(f) => f,
                    Err(e) => return Some(Err(DatabaseError::BoltTypes(e))),
                },
                _ => unreachable!("must be freelist page"),
            };
            for &i in &freelist {
                self.stack.push(PageIterItem {
                    parent_page_id: None,
                    page_id: i.into(),
                    typ: PageType::Free,
                });
            }

            let capacity = data.data.capacity();
            let used = data.data.used();
            return Some(Ok(PageInfo {
                id: item.page_id,
                typ: PageType::Freelist,
                overflow: page.overflow as u64,
                capacity: capacity as u64,
                used: used as u64,
                parent_page_id: None,
            }));
        }

        let page: bolt::PageHeader = match TryFrom::try_from(data.data.as_slice()) {
            Ok(p) => p,
            Err(e) => return Some(Err(DatabaseError::BoltTypes(e))),
        };
        match data.elem.as_ref().expect("must be leaf or branch") {
            Element::Branch(branch_elements) => {
                for branch_item in branch_elements {
                    self.stack.push(PageIterItem {
                        parent_page_id: Some(item.page_id),
                        page_id: branch_item.pgid.into(),
                        typ: PageType::DataBranch,
                    });
                }

                let capacity = data.data.capacity();
                let used = data.data.used();
                Some(Ok(PageInfo {
                    id: item.page_id,
                    typ: PageType::DataBranch,
                    overflow: data.overflow,
                    capacity: capacity as u64,
                    used: used as u64,
                    parent_page_id: item.parent_page_id,
                }))
            }
            Element::Leaf(leaf_elements) => {
                for leaf_item in leaf_elements {
                    if let boltypes::LeafElement::Bucket {
                        name: _,
                        pgid: pg_id,
                    } = leaf_item
                    {
                        self.stack.push(PageIterItem {
                            parent_page_id: Some(item.page_id),
                            page_id: Into::<u64>::into(*pg_id),
                            typ: PageType::DataLeaf,
                        });
                    }
                }

                let capacity = data.data.capacity();
                let used = data.data.used();
                Some(Ok(PageInfo {
                    id: item.page_id,
                    typ: PageType::DataLeaf,
                    overflow: page.overflow as u64,
                    capacity: capacity as u64,
                    used: used as u64,
                    parent_page_id: item.parent_page_id,
                }))
            }
        }
    }
}

struct BucketIterator<T: Iterator<Item = Result<DbItem, DatabaseError>>> {
    iter: T,
}

struct IterItem {
    parent_bucket: Option<Bucket>,
    node: ItemNode,
    index: usize,
    depth: Option<u64>,
}

enum ItemNode {
    Page(bolt::Pgid),
    Elements(Vec<KeyValue>),
}

impl<T> Iterator for BucketIterator<T>
where
    T: Iterator<Item = Result<DbItem, DatabaseError>>,
{
    type Item = Result<Bucket, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.iter.next();

            match item {
                None => return None,
                Some(Ok(db_item)) => match db_item {
                    DbItem::InlineBucket(bucket) => return Some(Ok(bucket)),
                    DbItem::KeyValue(_) => continue,
                    DbItem::Bucket(bucket) => return Some(Ok(bucket)),
                },
                Some(Err(e)) => return Some(Err(e)),
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
    page_size: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde::Serialize;
    use std::fs;
    use std::path::Path;

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(deny_unknown_fields)]
    struct Bucket {
        #[serde(rename = "Name")]
        name: String,
        #[serde(rename = "Items")]
        #[serde(default)]
        items: Vec<Item>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(rename_all = "PascalCase", tag = "Type")]
    enum Item {
        #[serde(rename = "kv")]
        KV {
            #[serde(rename = "Key")]
            key: String,
            #[serde(rename = "Value")]
            value: String,
        },
        #[serde(rename = "bucket")]
        Bucket {
            #[serde(rename = "Bucket")]
            bucket: Bucket,
        },
    }

    fn assert_buckets_equal<T>(depth: u64, parent: &String, iter: &mut T, expect_buckets: &[Bucket])
    where
        T: Iterator<Item = Result<super::Bucket, DatabaseError>>,
    {
        for (i, expect) in expect_buckets.iter().enumerate() {
            match iter.next() {
                None => {
                    panic!("want bucket at {i} but got nothing under: {parent}");
                }
                Some(Ok(actual)) => {
                    assert_eq!(
                        String::from_utf8(actual.name).unwrap(),
                        expect.name,
                        "different child bucket name under: {parent}"
                    );
                    assert_eq!(
                        depth, actual.depth,
                        "different child bucket depth under: {parent}",
                    );

                    #[allow(clippy::manual_filter_map)]
                    let expect_child_buckets: Vec<_> = expect
                        .clone()
                        .items
                        .into_iter()
                        .filter(|item| matches!(item, Item::Bucket { .. }))
                        .map(|item| match item {
                            Item::Bucket { bucket: v } => v,
                            _ => unreachable!(),
                        })
                        .collect();
                    assert_buckets_equal(
                        depth + 1,
                        &format!("{}/{}", parent, expect.name),
                        iter,
                        expect_child_buckets.as_slice(),
                    );
                }
                Some(Err(e)) => panic!("want item at {i} but got err {e} under: {parent}"),
            }
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
                .page_size(None)
                .build(),
        )
        .expect("open db successfully");
        let mut iter = db.iter_buckets();

        let content =
            fs::read_to_string(format!("{}/testdata/data.json", root_dir.to_str().unwrap()))
                .expect("Unable to read file");
        let expect_buckets: Vec<Bucket> = serde_json::from_str(&content).unwrap();

        assert_buckets_equal(0, &String::from(""), &mut iter, &expect_buckets);
    }

    fn assert_child_items_equal<T>(depth: u64, parent: &String, iter: &mut T, expect_items: &[Item])
    where
        T: Iterator<Item = Result<super::DbItem, DatabaseError>>,
    {
        for (i, expect) in expect_items.iter().enumerate() {
            let n = iter.next();
            if n.is_none() {
                panic!("want item at {i} but got nothing under: {parent}");
            }
            let n = n.unwrap();

            match expect {
                Item::KV { key, value } => match n {
                    Ok(super::DbItem::KeyValue(kv)) => {
                        assert_eq!(
                            String::from_utf8(kv.key.clone()).unwrap(),
                            *key,
                            "different key name under: {parent}"
                        );
                        assert_eq!(
                            String::from_utf8(kv.value).unwrap(),
                            *value,
                            "different key's value name under: {parent}, key: {key}"
                        );
                        assert_eq!(
                            depth, kv.depth,
                            "different child bucket's item depth under: {parent}, key: {key}",
                        );
                    }
                    _ => {
                        panic!("want kv item at {i} but got another under: {parent}");
                    }
                },
                Item::Bucket { bucket } => match n {
                    Ok(super::DbItem::Bucket(actual)) => {
                        assert_eq!(
                            String::from_utf8(actual.name).unwrap(),
                            bucket.name,
                            "different child bucket name under: {parent}"
                        );
                        assert_eq!(
                            depth, actual.depth,
                            "different child bucket depth under: {parent}, key: {}",
                            bucket.name
                        );

                        assert_child_items_equal(
                            depth + 1,
                            &format!("{}/{}", parent, bucket.name),
                            iter,
                            bucket.items.as_slice(),
                        );
                    }
                    Ok(super::DbItem::InlineBucket(actual)) => {
                        assert_eq!(
                            String::from_utf8(actual.name).unwrap(),
                            bucket.name,
                            "different child bucket name under: {parent}"
                        );
                        assert_eq!(
                            depth, actual.depth,
                            "different child bucket depth under: {parent}, key: {}",
                            bucket.name
                        );

                        assert_child_items_equal(
                            depth + 1,
                            &format!("{}/{}", parent, bucket.name),
                            iter,
                            bucket.items.as_slice(),
                        );
                    }
                    _ => {
                        panic!("want bucket item at {i} but got another under: {parent}",);
                    }
                },
            }
        }
    }

    fn assert_items_equal<T>(depth: u64, parent: &String, iter: &mut T, expect_buckets: &[Bucket])
    where
        T: Iterator<Item = Result<super::DbItem, DatabaseError>>,
    {
        for (i, expect) in expect_buckets.iter().enumerate() {
            match iter.next() {
                None => {
                    panic!("want item at {i} but got nothing under: {parent}");
                }
                Some(Ok(DbItem::Bucket(actual))) => {
                    assert_eq!(
                        String::from_utf8(actual.name).unwrap(),
                        expect.name,
                        "different child bucket name under: {parent}"
                    );
                    assert_eq!(
                        depth, actual.depth,
                        "different child bucket depth under: {parent}, key: {}",
                        expect.name
                    );

                    assert_child_items_equal(
                        depth + 1,
                        &format!("{}/{}", parent, expect.name),
                        iter,
                        expect.items.as_slice(),
                    );
                }
                Some(Ok(DbItem::InlineBucket(ref actual))) => {
                    assert_eq!(
                        String::from_utf8(actual.name.clone()).unwrap(),
                        expect.name,
                        "different child bucket name under: {parent}"
                    );
                    assert_eq!(
                        depth, actual.depth,
                        "different child bucket depth under: {parent}, key: {}",
                        expect.name,
                    );

                    assert_child_items_equal(
                        depth + 1,
                        &format!("{}/{}", parent, expect.name),
                        iter,
                        expect.items.as_slice(),
                    );
                }
                Some(Ok(DbItem::KeyValue(_))) => {
                    panic!("want bucket item at {i} but got kvs: {parent}");
                }
                Some(Err(e)) => panic!("want item at {i} but got err {e} under: {parent}"),
            }
        }
    }

    #[test]
    fn test_iter_items() {
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
                .page_size(None)
                .build(),
        )
        .expect("open db successfully");
        let mut iter = db.iter_items();

        let content =
            fs::read_to_string(format!("{}/testdata/data.json", root_dir.to_str().unwrap()))
                .expect("Unable to read file");
        let expect_buckets: Vec<Bucket> = serde_json::from_str(&content).unwrap();

        assert_items_equal(0, &String::from(""), &mut iter, &expect_buckets);
    }

    #[test]
    fn test_multi_thread() {
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
                .page_size(None)
                .build(),
        )
        .expect("open db successfully");

        let mut handles = vec![];
        let result = Arc::new(Mutex::new(vec![0, 0, 0]));
        for thread_id in 0..3 {
            let db_clone = db.clone();
            let result_clone = result.clone();
            handles.push(std::thread::spawn(move || {
                let mut result = result_clone.lock().unwrap();
                if thread_id == 0 {
                    result[0] = db_clone.iter_pages().count();
                } else if thread_id == 1 {
                    result[1] = db_clone.iter_buckets().count();
                } else {
                    result[2] = db_clone.iter_items().count();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(result.lock().unwrap().as_slice(), vec![80, 396, 1385]);
    }

    #[test]
    fn test_pages() {
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
                .page_size(None)
                .build(),
        )
        .expect("open db successfully");

        let actual_pages: Vec<PageInfo> = db.iter_pages().map(|x| x.unwrap()).collect();

        let content =
            fs::read_to_string(format!("{}/testdata/page.json", root_dir.to_str().unwrap()))
                .expect("Unable to read file");
        let expect_pages: Vec<PageInfo> = serde_json::from_str(&content).unwrap();
        assert_eq!(actual_pages, expect_pages);
    }
}
