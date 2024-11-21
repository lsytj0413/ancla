use crate::bolt::{self, PAGE_HEADER_SIZE};
use bitflags::iter::Iter;
use bitflags::Flags;
use fnv_rs::{Fnv64, FnvHasher};
use prettytable::Table;
use std::cell::RefCell;
use std::ops::{Deref, IndexMut};
use std::rc::Rc;
use std::sync::Arc;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read, Seek},
    ops::Index,
};

use tui::{
    backend::CrosstermBackend,
    widgets::{Block, Borders},
    Terminal,
};
use typed_builder::TypedBuilder;

pub struct DB {
    pub(crate) options: AnclaOptions,
    file: File,

    pages: BTreeMap<bolt::Pgid, PageInfo>,
    page_datas: BTreeMap<bolt::Pgid, Arc<Vec<u8>>>,
    meta0: Option<bolt::Meta>,
    meta1: Option<bolt::Meta>,
}

#[derive(Debug, Clone, Copy)]
pub struct PageInfo {
    pub id: u64,
    pub typ: PageType,
    pub overflow: u64,
    pub capacity: u64,
    pub used: u64,
    pub parent_page_id: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ItemInfo {
    pub page: u64,
    pub typ: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
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

#[derive(Debug, Clone, Copy)]
pub enum PageType {
    Meta,
    Data,
    Freelist,
    Free,
}

#[derive(Debug, Clone)]
struct BranchElement {
    key: Vec<u8>,
    pgid: u64,
}

#[derive(Debug, Clone)]
enum LeafElement {
    Bucket { name: Vec<u8>, pgid: u64 },
    InlineBucket { name: Vec<u8>, items: Vec<KeyValue> },
    KeyValue(KeyValue),
}

#[derive(Debug, Clone)]
struct KeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
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

    fn read_page(&mut self, page_id: u64) -> Arc<Vec<u8>> {
        if let Some(data) = self.page_datas.get(&From::from(page_id)) {
            return Arc::clone(data);
        }

        let data = self.read(page_id * 4096, PAGE_HEADER_SIZE);
        let page: bolt::Page = TryFrom::try_from(data.as_slice()).unwrap();

        let data_len = 4096 * (page.overflow + 1) as usize;
        let data = self.read(page_id * 4096, data_len);
        let data = Arc::new(data);
        self.page_datas
            .insert(From::from(page_id), Arc::clone(&data));
        Arc::clone(&data)
    }

    fn read_page_elements(&mut self, page_id: u64) {}

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

    fn read_meta_page(&mut self, data: &[u8]) -> bolt::Meta {
        let page: bolt::Page = TryFrom::try_from(data).unwrap();
        if !page.flags.contains(bolt::PageFlag::MetaPageFlag) {
            panic!(
                "read_page_overflow: page 0 is not a meta page, expect flag {}, got {}",
                bolt::PageFlag::MetaPageFlag.as_u16(),
                page.flags.as_u16()
            );
        }
        let actual_checksum =
            u64::from_be_bytes(Fnv64::hash(&data[16..72]).as_bytes().try_into().unwrap());
        let meta: bolt::Meta = TryFrom::try_from(data).unwrap();
        if meta.checksum != actual_checksum {
            panic!(
                "checksum mismatch, expect {}, got {}",
                actual_checksum, meta.checksum
            );
        }
        if meta.magic != bolt::MAGIC_NUMBER {
            panic!(
                "invalid magic number, expect {}, got {}",
                bolt::MAGIC_NUMBER,
                meta.magic
            );
        }
        if meta.version != bolt::DATAFILE_VERSION {
            panic!(
                "invalid version number, expect {}, got {}",
                bolt::DATAFILE_VERSION,
                meta.version
            );
        }
        meta
    }

    fn initialize(&mut self) {
        let data0 = self.read_page(0);
        let meta0 = self.read_meta_page(&data0);
        self.meta0 = Some(meta0);

        let data1 = self.read_page(1);
        let meta1 = self.read_meta_page(&data1);
        self.meta1 = Some(meta1);
    }

    fn get_meta(&mut self) -> bolt::Meta {
        if self.meta0.is_none() && self.meta1.is_none() {
            panic!("meta0 and meta1 are not initialized");
        }

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

    fn read_leaf_element(
        &mut self,
        page: &[u8],
        count: u16,
        page_id: u64,
        parent_page_id: Option<u64>,
    ) {
        for i in 0..count {
            let start = (16 + i * 16) as usize;
            let leaf_element: bolt::LeafPageElement =
                bolt::LeafPageElement::try_from(page.get(start..page.len()).unwrap()).unwrap();

            let key_start = 16 + i * 16 + (leaf_element.pos as u16);
            let key_end = key_start + (leaf_element.ksize as u16);
            let key = page.get((key_start as usize)..(key_end as usize)).unwrap();
            let value = page
                .get((key_end as usize)..((key_end + leaf_element.vsize as u16) as usize))
                .unwrap();
            println!(
                "flag: {}, key: {}, value: {}, key_size: {}, value_size: {}",
                leaf_element.flags,
                String::from_utf8(key.to_vec()).unwrap(),
                String::from_utf8(value.to_vec()).unwrap(),
                leaf_element.ksize,
                leaf_element.vsize,
            );

            if leaf_element.flags == 0x01 {
                let bucket_page_id = self.read_page_u64(&Vec::from(value), 0);
                println!("bucket_page_id: {}", bucket_page_id);
                if bucket_page_id == 0 {
                    // This is an inline bucket, so we need to read the bucket data
                    let data = &value[16..];
                    let page: bolt::Page = TryFrom::try_from(data).unwrap();
                    println!(
                        "bucket_page_id: {}, page_count: {}",
                        bucket_page_id, page.count
                    );
                    self.read_leaf_element(data, page.count, page_id, parent_page_id);
                    continue;
                }

                self.print_page(bucket_page_id, Some(page_id));
            }
        }
    }

    fn read_branch_element(
        &mut self,
        page: &[u8],
        count: u16,
        page_id: u64,
        _parent_page_id: Option<u64>,
    ) {
        for i in 0..count {
            let start = (16 + i * 16) as usize;
            let branch_element: bolt::BranchPageElement =
                bolt::BranchPageElement::try_from(page.get(start..page.len()).unwrap()).unwrap();
            self.print_page(branch_element.pgid.into(), Some(page_id));
        }
    }

    pub fn build(ancla_options: AnclaOptions) -> Rc<RefCell<DB>> {
        let file = File::open(ancla_options.db_path.clone()).unwrap();
        Rc::new(RefCell::new(DB {
            options: ancla_options,
            file,
            pages: BTreeMap::new(),
            page_datas: BTreeMap::new(),
            meta0: None,
            meta1: None,
        }))
    }

    pub fn print_page(&mut self, page_id: u64, parent_page_id: Option<u64>) {
        let data = self.read_page(page_id);
        let page: bolt::Page = TryFrom::try_from(data.as_slice()).unwrap();
        self.pages.insert(
            bolt::Pgid(page_id),
            PageInfo {
                id: page_id,
                typ: PageType::Data,
                overflow: page.overflow as u64,
                capacity: 4096 * (page.overflow + 1) as u64,
                used: 0,
                parent_page_id,
            },
        );

        if page.flags.as_u16() == 0x02 {
            // leaf page
            self.read_leaf_element(&data, page.count, page_id, parent_page_id);
        } else if page.flags.as_u16() == 0x01 {
            // branch page
            self.read_branch_element(&data, page.count, page_id, parent_page_id);
        }
    }

    pub fn print_db(&mut self) {
        self.initialize();
        let meta = self.get_meta();

        self.pages.insert(
            bolt::Pgid(0),
            PageInfo {
                id: 0,
                typ: PageType::Meta,
                overflow: 0,
                capacity: 4096,
                used: 80,
                parent_page_id: None,
            },
        );
        self.pages.insert(
            bolt::Pgid(1),
            PageInfo {
                id: 1,
                typ: PageType::Meta,
                overflow: 0,
                capacity: 4096,
                used: 80,
                parent_page_id: None,
            },
        );

        println!("Active root page: {:?}", meta);
        let data = self.read_page(meta.freelist_pgid.into());
        let freelist_page: bolt::Page = TryFrom::try_from(data.as_slice()).unwrap();
        if !freelist_page
            .flags
            .contains(bolt::PageFlag::FreelistPageFlag)
        {
            panic!("Invalid freelist page type")
        }
        if freelist_page.count == 0xFFFF {
            panic!("Too large page count")
        }
        self.pages.insert(
            bolt::Pgid(meta.freelist_pgid.into()),
            PageInfo {
                id: meta.freelist_pgid.into(),
                typ: PageType::Freelist,
                overflow: freelist_page.overflow as u64,
                capacity: 4096,
                used: 16 + (freelist_page.count as u64 * 8),
                parent_page_id: None,
            },
        );

        let freelist = self.read_freelist(&data, freelist_page.count);
        // See
        // 1. https://stackoverflow.com/questions/59123462/why-is-iterating-over-a-collection-via-for-loop-considered-a-move-in-rust
        // 2. https://doc.rust-lang.org/reference/expressions/loop-expr.html#iterator-loops
        for &i in &freelist {
            self.pages.insert(
                bolt::Pgid(i),
                PageInfo {
                    id: i,
                    typ: PageType::Free,
                    overflow: 0,
                    capacity: 4096,
                    used: 0,
                    parent_page_id: None,
                },
            );
        }
        println!("Freelist: {:?}", freelist);

        let data = self.read_page(meta.root_pgid.into());
        let root_page: bolt::Page = TryFrom::try_from(data.as_slice()).unwrap();
        if root_page.flags.as_u16() != 0x02 && root_page.flags.as_u16() != 0x01 {
            panic!("Invalid root page's type")
        }

        self.print_page(root_page.id.into(), None);

        for (&key, &value) in &self.pages {
            println!(" {:?} {:?}", key, value);
        }

        let mut table = Table::new();
        table.add_row(row!["PageSize", "value"]);
        table.add_row(row![10]);
        table.printstd();

        let stdout = io::stdout();
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| {
                let size = f.size();
                let block = Block::default().title("Ancla").borders(Borders::ALL);
                f.render_widget(block, size);
            })
            .unwrap();
    }

    pub fn iter_buckets(db: Rc<RefCell<DB>>) -> impl Iterator<Item = Bucket> {
        db.borrow_mut().initialize();
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

    pub fn iter_pages(&mut self) -> impl Iterator<Item = PageInfo> {
        self.initialize();
        let meta = self.get_meta();

        let pages = vec![
            PageInfo {
                id: 0,
                typ: PageType::Meta,
                overflow: 0,
                capacity: 4096,
                used: 80,
                parent_page_id: None,
            },
            PageInfo {
                id: 1,
                typ: PageType::Meta,
                overflow: 0,
                capacity: 4096,
                used: 80,
                parent_page_id: None,
            },
        ];
        pages.into_iter()
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
            let page: bolt::Page = TryFrom::try_from(data.as_slice()).unwrap();
            if page.flags.contains(bolt::PageFlag::LeafPageFlag) {
                let leaf_elements = self.db.borrow_mut().read_page_leaf_elements(&data);
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
            } else if page.flags.contains(bolt::PageFlag::BranchPageFlag) {
                let branch_elements = self.db.borrow_mut().read_page_branch_elements(&data);
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

// bucket -- list all bucket
// check -- is page double free、is all page reachable
// compact --
// dump -- print pages
// page-item -- print page items
// get -- print key value
// info -- print page size
// keys -- print keys
// page -- print pages
// stats -- ....
// surgery --
// print etcd's interval data

#[derive(TypedBuilder)]
pub struct AnclaOptions {
    db_path: String,
}
