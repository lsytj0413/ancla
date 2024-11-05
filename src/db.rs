use bitflags::bitflags;
use fnv_rs::{Fnv64, FnvHasher};
use prettytable::Table;
use std::{
    fs::File,
    io::{self, Read, Seek},
    ptr,
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
}

#[derive(Debug)]
#[repr(C)]
struct Page {
    // is the identifier of the page, it start from 0,
    // and is incremented by 1 for each page.
    // There have two special pages:
    //   - 0: meta0
    //   - 1: meta1
    // The are the root page of database, and the meta (valid) which have bigger
    // txid is current available.
    id: Pgid,
    // indicate which type this page is.
    flags: PageFlag,
    // number of element in this page, if the page is freelist page:
    // 1. if value < 0xFFFF, it's the number of pageid
    // 2. if value is 0xFFFF, the next 8-bytes（page's offset 16） is the number of pageid.
    count: u16,
    // the continous number of page, all page's data is stored in the buffer which
    // size is (1 + overflow) * PAGE_SIZE.
    overflow: u32,
}

#[derive(Debug, PartialEq, PartialOrd)]
#[repr(transparent)]
#[derive(Clone)]
struct Pgid(u64);

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct PageFlag: u16 {
        // Branch page contains the branch element, which represent the
        // sub page and it's minest key value.
        const BranchPageFlag = 0x01;
        // Leaf page contains the leaf element, which represent the
        // key、value pair, and the element maybe bucket or not.
        const LeafPageFlag = 0x02;
        // Meta page contains the meta information about the database,
        // it must be page 0 or 1.
        const MetaPageFlag = 0x04;
        // Freelist page contains all pageid which is free to be used.
        const FreelistPageFlag = 0x10;
    }
}

impl PageFlag {
    pub fn as_u16(&self) -> u16 {
        self.bits() as u16
    }
}

#[derive(Debug)]
#[repr(C)]
struct Meta {
    // The magic number of bolt database, must be MAGIC_NUMBER.
    magic: u32,
    // Database file format version, must be DATAFILE_VERSION.
    version: u32,
    // Size in bytes of each page.
    page_size: u32,
    _flag: u32, // unused
    // Rust doesn't have `type embedding` that Go has, see
    // https://github.com/rust-lang/rfcs/issues/2431 for more detail.
    // The root data pageid of the database.
    root_pgid: Pgid,
    root_sequence: u64,
    // The root freelist pageid of the database.
    freelist_pgid: Pgid,
    // The max pageid of the database, it shoule be FILE_SIZE / PAGE_SIZE.
    max_pgid: Pgid,
    // current max txid of the databse, there have two Meta page, which have bigger txid
    // is valid.
    txid: u64,
    checksum: u64,
}

// Represents a marker value to indicate that a file is a Bolt DB.
const MAGIC_NUMBER: u32 = 0xED0CDAED;

// The data file format version.
const DATAFILE_VERSION: u32 = 2;

#[derive(Debug)]
#[repr(C)]
struct BranchPageElement {
    // pos is the offset of the element's data in the page,
    // start at current element's position.
    pos: u32,
    // the key's length in bytes.
    ksize: u32,
    // the next-level pageid.
    pgid: Pgid,
}

#[derive(Debug)]
#[repr(C)]
struct LeafPageElement {
    // indicate what type of the element, if flags is 1, it's a bucket,
    // otherwise it's a key-value pair.
    flags: u32,
    // pos is the offset of the element's data in the page,
    // start at current element's position.
    pos: u32,
    // the key's length in bytes.
    ksize: u32,
    // the value's length in bytes.
    vsize: u32,
}

#[derive(Debug)]
#[repr(C)]
// Bucket represents the on-file representation of a bucket. It is stored as
// the `value` of a bucket key. If the root is 0, this bucket is small enough
// then it's root page can be stored inline in the value, just after the bucket header.
struct Bucket {
    // the bucket's root-level page.
    root: Pgid,
    sequence: u64,
}

impl DB {
    fn read_page(&mut self, page_id: u64) -> Vec<u8> {
        let mut data = vec![0u8; 4096];
        self.file
            .seek(io::SeekFrom::Start((page_id * 4096) as u64))
            .unwrap();

        let size = self.file.read(data.as_mut_slice()).unwrap();
        if size != 4096 {
            panic!("Invalid page size");
        }
        data
    }

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

    fn read_page_flag(&mut self, page: &Vec<u8>) -> u16 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(8) as *const u8;
            let value_ptr = std::slice::from_raw_parts(offset_ptr, 2);
            let value: u16 = u16::from_le_bytes(value_ptr.try_into().unwrap());
            value
        }
    }

    fn read_page_overflow_value(&mut self, page: &Vec<u8>) -> u32 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(12) as *const u8;
            let value_ptr = std::slice::from_raw_parts(offset_ptr, 4);
            let value: u32 = u32::from_le_bytes(value_ptr.try_into().unwrap());
            value
        }
    }

    fn read_meta_page_size(&mut self, page: &Vec<u8>) -> u32 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(24) as *const u32;
            return offset_ptr.read_unaligned();
        }
    }

    fn read_page_count(&mut self, page: &Vec<u8>) -> u16 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(10) as *const u16;
            return offset_ptr.read_unaligned();
        }
    }

    fn read_meta_checksum(&mut self, page: &Vec<u8>) -> u64 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(72) as *const u64;
            return offset_ptr.read_unaligned();
        }
    }

    fn read_meta_txid(&mut self, page: &Vec<u8>) -> u64 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(64) as *const u64;
            return offset_ptr.read_unaligned();
        }
    }

    fn read_meta_freelist_pgid(&mut self, page: &Vec<u8>) -> u64 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(48) as *const u64;
            return offset_ptr.read_unaligned();
        }
    }

    fn read_meta_root(&mut self, page: &Vec<u8>) -> u64 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(32) as *const u64;
            return offset_ptr.read_unaligned();
        }
    }

    fn read_page_u64(&mut self, page: &Vec<u8>, offset: u16) -> u64 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(offset as isize) as *const u64;
            return offset_ptr.read_unaligned();
        }
    }

    fn read_page_u32(&mut self, page: &Vec<u8>, offset: u16) -> u32 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(offset as isize) as *const u32;
            return offset_ptr.read_unaligned();
        }
    }

    fn read_freelist(&mut self, page: &Vec<u8>, count: u16) -> Vec<u64> {
        let mut freelist: Vec<u64> = Vec::with_capacity(count as usize);
        for i in 0..count {
            freelist.push(self.read_page_u64(page, i * 8 + 16));
        }
        freelist
    }

    fn read_leaf_element(&mut self, page: &Vec<u8>, count: u16) {
        for i in 0..count {
            let flag = self.read_page_u32(page, 16 + i * 16);
            let pos = self.read_page_u32(page, 16 + i * 16 + 4);
            let key_len = self.read_page_u32(page, 16 + i * 16 + 8);
            let value_len = self.read_page_u32(page, 16 + i * 16 + 12);

            let key_start = 16 + i * 16 + (pos as u16);
            let key_end = key_start + (key_len as u16);
            let key = page.get((key_start as usize)..(key_end as usize)).unwrap();
            let value = page
                .get((key_end as usize)..((key_end + value_len as u16) as usize))
                .unwrap();
            println!(
                "flag: {}, key: {}, value: {}, key_size: {}, value_size: {}",
                flag,
                String::from_utf8(key.to_vec()).unwrap(),
                String::from_utf8(value.to_vec()).unwrap(),
                key_len,
                value_len,
            );

            if flag == 0x01 {
                let bucket_page_id = self.read_page_u64(&Vec::from(value), 0);
                println!("bucket_page_id: {}", bucket_page_id);
                if bucket_page_id == 0 {
                    // This is an inline bucket, so we need to read the bucket data
                    let data = value[16..].to_vec();
                    let page_count = self.read_page_count(&data);
                    println!(
                        "bucket_page_id: {}, page_count: {}",
                        bucket_page_id, page_count
                    );
                    self.read_leaf_element(&data, page_count);
                    continue;
                }

                self.print_page(bucket_page_id);
            }
        }
    }

    fn read_branch_element(&mut self, page: &Vec<u8>, count: u16) {
        for i in 0..count {
            let next_page_id = self.read_page_u64(page, 16 + i * 16 + 8);
            self.print_page(next_page_id);
        }
    }

    pub fn build(ancla_options: AnclaOptions) -> DB {
        let file = File::open(ancla_options.db_path.clone()).unwrap();
        DB {
            options: ancla_options,
            file,
        }
    }

    pub fn print_page(&mut self, page_id: u64) {
        if Pgid(0) < Pgid(1) {}

        let data = self.read_page(page_id);
        let page_flag = self.read_page_flag(&data);
        let page_count = self.read_page_count(&data);
        let page_overflow = self.read_page_overflow_value(&data);
        println!(
            "Page ID: {}, flag: {}, count: {}, overflow: {}",
            page_id, page_flag, page_count, page_overflow
        );

        let data = self.read_page_overflow(page_id, page_overflow);
        if page_flag == 0x02 {
            // leaf page
            self.read_leaf_element(&data, page_count);
        } else if page_flag == 0x01 {
            // branch page
            self.read_branch_element(&data, page_count);
        }
    }

    pub fn print_db(&mut self) {
        let data = self.read_page(0);
        let size = data.capacity();
        println!("{}, {:?}", size, &data[16..20]);
        let page_flag = self.read_page_flag(&data);
        println!(
            "first page flag: {}, {}",
            page_flag,
            self.read_meta_page_size(&data)
        );
        if page_flag != 0x04 {
            panic!("Invalid page 0's type")
        }

        let hash = Fnv64::hash(&data[16..72]);
        println!("page 0 hash: {}", hash);
        let hash_data = hash.as_bytes();
        println!("hash data: {}", hash.len());
        let new_checksum = u64::from_be_bytes(hash_data.try_into().unwrap());

        let checksum = self.read_meta_checksum(&data);
        if checksum != new_checksum {
            panic!(
                "Invalid page 0's checksum, {:0x} != {:0x}",
                checksum, new_checksum
            );
        }

        let meta0_txid = self.read_meta_txid(&data);
        let meta0_freelist_pgid = self.read_meta_freelist_pgid(&data);
        let meta0_root_pgid = self.read_meta_root(&data);

        let data = self.read_page(1);
        let size = data.capacity();
        println!("{}, {:?}", size, &data[16..20]);
        let page_flag = self.read_page_flag(&data);
        println!(
            "second page flag: {}, {}",
            page_flag,
            self.read_meta_page_size(&data)
        );
        if page_flag != 0x04 {
            panic!("Invalid page 1's type")
        }

        let meta1_txid = self.read_meta_txid(&data);
        let meta1_freelist_pgid = self.read_meta_freelist_pgid(&data);
        let meta1_root_pgid = self.read_meta_root(&data);

        let freelist_pgid = if meta1_txid > meta0_txid {
            meta1_freelist_pgid
        } else {
            meta0_freelist_pgid
        };

        println!("Freelist root page: {}", freelist_pgid);
        let data = self.read_page(freelist_pgid);
        let count = self.read_page_count(&data);
        if count >= 0xFFFF {
            panic!("Too large page count")
        }

        let data = self.read_page(freelist_pgid);
        let page_flag = self.read_page_flag(&data);
        if page_flag != 0x10 {
            panic!("Invalid freelist page's type")
        }

        let freelist = self.read_freelist(&data, count);
        println!("Freelist: {:?}", freelist);

        println!(
            "meta0 root: {}, meta1 root: {}",
            meta0_root_pgid, meta1_root_pgid
        );
        let root_pgid = if meta1_txid > meta0_txid {
            meta1_root_pgid
        } else {
            meta0_root_pgid
        };

        println!("Root page: {}", root_pgid);
        let data = self.read_page(root_pgid);
        let page_flag = self.read_page_flag(&data);
        if page_flag != 0x02 {
            panic!("Invalid root page's type")
        }

        self.print_page(root_pgid);
        // let count = self.read_page_count(&data);
        // self.read_leaf_element(&data, count);

        let mut table = Table::new();
        table.add_row(row!["PageSize", "value"]);
        table.add_row(row![size]);
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
