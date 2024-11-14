use crate::bolt;
use bitflags::Flags;
use fnv_rs::{Fnv64, FnvHasher};
use prettytable::Table;
use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read, Seek},
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
    page_datas: BTreeMap<bolt::Pgid, Vec<u8>>,
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
    pub bucket: BucketInfo,
}

#[derive(Debug, Clone)]
pub struct BucketInfo {
    pub page: u64,
    pub is_inline: bool,
    pub key: Vec<u8>,
    pub items: Vec<ItemInfo>,
}

#[derive(Debug, Clone, Copy)]
pub enum PageType {
    MetaPage,
    DataPage,
    FreelistPage,
    FreePage,
}

// Represents a marker value to indicate that a file is a Bolt DB.
const MAGIC_NUMBER: u32 = 0xED0CDAED;

// The data file format version.
const DATAFILE_VERSION: u32 = 2;

impl DB {
    fn read_page_overflow(&mut self, page_id: u64, overflow: u32) -> Vec<u8> {
        let data_len = 4096 * (overflow + 1) as usize;
        let mut data = vec![0u8; data_len];
        self.file
            .seek(io::SeekFrom::Start((page_id * 4096) as u64))
            .unwrap();
        let size = self.file.read(data.as_mut_slice()).unwrap();
        if size != data_len {
            panic!(
                "read_page_overflow: read {} bytes, expected {}",
                size, data_len
            );
        }
        data
    }

    fn read_page_u64(&mut self, page: &Vec<u8>, offset: u16) -> u64 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(offset as isize) as *const u8;
            let value_ptr = std::slice::from_raw_parts(offset_ptr, 8);
            u64::from_le_bytes(value_ptr.try_into().unwrap())
        }
    }

    fn read_freelist(&mut self, page: &Vec<u8>, count: u16) -> Vec<u64> {
        let mut freelist: Vec<u64> = Vec::with_capacity(count as usize);
        for i in 0..count {
            freelist.push(self.read_page_u64(page, i * 8 + 16));
        }
        freelist
    }

    fn read_leaf_element(
        &mut self,
        page: &Vec<u8>,
        count: u16,
        page_id: u64,
        parent_page_id: Option<u64>,
    ) {
        for i in 0..count {
            let start = (16 + i * 16) as usize;
            let leaf_element: bolt::LeafPageElement =
                bolt::LeafPageElement::try_from(&page.get(start..page.len()).unwrap().to_vec())
                    .unwrap();

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
                    let data = value[16..].to_vec();
                    let page: bolt::Page = TryFrom::try_from(&data).unwrap();
                    println!(
                        "bucket_page_id: {}, page_count: {}",
                        bucket_page_id, page.count
                    );
                    self.read_leaf_element(&data, page.count, page_id, parent_page_id);
                    continue;
                }

                self.print_page(bucket_page_id, Some(page_id));
            }
        }
    }

    fn read_branch_element(
        &mut self,
        page: &Vec<u8>,
        count: u16,
        page_id: u64,
        _parent_page_id: Option<u64>,
    ) {
        for i in 0..count {
            let start = (16 + i * 16) as usize;
            let branch_element: bolt::BranchPageElement =
                bolt::BranchPageElement::try_from(&page.get(start..page.len()).unwrap().to_vec())
                    .unwrap();
            self.print_page(branch_element.pgid.into(), Some(page_id));
        }
    }

    pub fn build(ancla_options: AnclaOptions) -> DB {
        let file = File::open(ancla_options.db_path.clone()).unwrap();
        DB {
            options: ancla_options,
            file,
            pages: BTreeMap::new(),
            page_datas: BTreeMap::new(),
        }
    }

    pub fn print_page(&mut self, page_id: u64, parent_page_id: Option<u64>) {
        let data = self.read_page_overflow(page_id, 0);
        let page: bolt::Page = TryFrom::try_from(&data).unwrap();
        println!("print page: {:?}, {:?}", page, parent_page_id);

        let data = self.read_page_overflow(page_id, page.overflow);
        self.pages.insert(
            bolt::Pgid(page_id),
            PageInfo {
                id: page_id,
                typ: PageType::DataPage,
                overflow: page.overflow as u64,
                capacity: 4096 * (page.overflow + 1) as u64,
                used: 0,
                parent_page_id: parent_page_id,
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

    fn for_page_buckets(&mut self, page_id: u64, f: fn(&Vec<u8>)) {
        let data = self.read_page_overflow(page_id, 0);
        let page: bolt::Page = TryFrom::try_from(&data).unwrap();
        println!("print page: {:?}", page);

        let data = self.read_page_overflow(page_id, page.overflow);
        if page.flags.as_u16() == 0x02 {
            // leaf page
            self.for_leaf_page_element(&data, page.count, page_id, f);
        } else if page.flags.as_u16() == 0x01 {
            // branch page
            self.for_branch_page_element(&data, page.count, page_id, f);
        }
    }

    fn for_leaf_page_element(&mut self, page: &Vec<u8>, count: u16, page_id: u64, f: fn(&Vec<u8>)) {
        for i in 0..count {
            let start = (16 + i * 16) as usize;
            let leaf_element: bolt::LeafPageElement =
                bolt::LeafPageElement::try_from(&page.get(start..page.len()).unwrap().to_vec())
                    .unwrap();

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
                f(&page
                    .get((key_start as usize)..(key_end as usize))
                    .unwrap()
                    .to_vec());

                let bucket_page_id = self.read_page_u64(&Vec::from(value), 0);
                println!("bucket_page_id: {}", bucket_page_id);
                if bucket_page_id == 0 {
                    // This is an inline bucket, so we need to read the bucket data
                    continue;
                }

                self.for_page_buckets(bucket_page_id, f);
            }
        }
    }

    fn for_branch_page_element(
        &mut self,
        page: &Vec<u8>,
        count: u16,
        page_id: u64,
        f: fn(&Vec<u8>),
    ) {
        for i in 0..count {
            let start = (16 + i * 16) as usize;
            let branch_element: bolt::BranchPageElement =
                bolt::BranchPageElement::try_from(&page.get(start..page.len()).unwrap().to_vec())
                    .unwrap();
            self.for_page_buckets(branch_element.pgid.into(), f);
        }
    }

    pub fn print_db(&mut self) {
        let data = self.read_page_overflow(0, 0);
        let page0: bolt::Page = TryFrom::try_from(&data).unwrap();
        println!("first page: {:?}", page0);
        if page0.flags.as_u16() != 0x04 {
            panic!("Invalid page 0's type")
        }

        let new_checksum =
            u64::from_be_bytes(Fnv64::hash(&data[16..72]).as_bytes().try_into().unwrap());
        let meta0: bolt::Meta = TryFrom::try_from(&data).unwrap();
        println!("first meta: {:?}", meta0);
        if meta0.checksum != new_checksum {
            panic!(
                "Invalid page 0's checksum, {:0x} != {:0x}",
                meta0.checksum, new_checksum
            );
        }
        self.pages.insert(
            bolt::Pgid(0),
            PageInfo {
                id: 0,
                typ: PageType::MetaPage,
                overflow: 0,
                capacity: 4096,
                used: 80,
                parent_page_id: None,
            },
        );

        let data = self.read_page_overflow(1, 0);
        let page1: bolt::Page = TryFrom::try_from(&data).unwrap();
        println!("second page: {:?}", page1);
        if page1.flags.as_u16() != 0x04 {
            panic!("Invalid page 0's type")
        }

        let meta1: bolt::Meta = TryFrom::try_from(&data).unwrap();
        println!("second meta: {:?}", meta1);
        let (page, meta) = if meta1.txid > meta0.txid {
            (page1, meta1)
        } else {
            (page0, meta0)
        };
        self.pages.insert(
            bolt::Pgid(1),
            PageInfo {
                id: 1,
                typ: PageType::MetaPage,
                overflow: 0,
                capacity: 4096,
                used: 80,
                parent_page_id: None,
            },
        );

        println!("Active root page: {:?} {:?}", page, meta);
        let data = self.read_page_overflow(meta.freelist_pgid.into(), 0);
        let freelist_page: bolt::Page = TryFrom::try_from(&data).unwrap();
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
                typ: PageType::FreelistPage,
                overflow: freelist_page.overflow as u64,
                capacity: 4096,
                used: 16 + (freelist_page.count as u64 * 8),
                parent_page_id: None,
            },
        );

        let data = self.read_page_overflow(meta.freelist_pgid.into(), freelist_page.overflow);
        let freelist = self.read_freelist(&data, freelist_page.count);
        // See
        // 1. https://stackoverflow.com/questions/59123462/why-is-iterating-over-a-collection-via-for-loop-considered-a-move-in-rust
        // 2. https://doc.rust-lang.org/reference/expressions/loop-expr.html#iterator-loops
        for &i in &freelist {
            self.pages.insert(
                bolt::Pgid(i),
                PageInfo {
                    id: i,
                    typ: PageType::FreePage,
                    overflow: 0,
                    capacity: 4096,
                    used: 0,
                    parent_page_id: None,
                },
            );
        }
        println!("Freelist: {:?}", freelist);

        let data = self.read_page_overflow(meta.root_pgid.into(), 0);
        let root_page: bolt::Page = TryFrom::try_from(&data).unwrap();
        if root_page.flags.as_u16() != 0x02 {
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

    pub fn print_buckets(&mut self) {
        let data = self.read_page_overflow(0, 0);
        let page0: bolt::Page = TryFrom::try_from(&data).unwrap();
        println!("first page: {:?}", page0);
        if page0.flags.as_u16() != 0x04 {
            panic!("Invalid page 0's type")
        }

        let new_checksum =
            u64::from_be_bytes(Fnv64::hash(&data[16..72]).as_bytes().try_into().unwrap());
        let meta0: bolt::Meta = TryFrom::try_from(&data).unwrap();
        println!("first meta: {:?}", meta0);
        if meta0.checksum != new_checksum {
            panic!(
                "Invalid page 0's checksum, {:0x} != {:0x}",
                meta0.checksum, new_checksum
            );
        }

        let data = self.read_page_overflow(1, 0);
        let page1: bolt::Page = TryFrom::try_from(&data).unwrap();
        println!("second page: {:?}", page1);
        if page1.flags.as_u16() != 0x04 {
            panic!("Invalid page 0's type")
        }

        let meta1: bolt::Meta = TryFrom::try_from(&data).unwrap();
        println!("second meta: {:?}", meta1);
        let (page, meta) = if meta1.txid > meta0.txid {
            (page1, meta1)
        } else {
            (page0, meta0)
        };

        println!("Active root page: {:?} {:?}", page, meta);

        let data = self.read_page_overflow(meta.root_pgid.into(), 0);
        let root_page: bolt::Page = TryFrom::try_from(&data).unwrap();
        if root_page.flags.as_u16() != 0x02 {
            panic!("Invalid root page's type")
        }
        self.for_page_buckets(meta.root_pgid.into(), |bucket| {
            println!("bucket: {:?}", String::from_utf8(bucket.to_vec()).unwrap());
        });
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
