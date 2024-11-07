use bitflags::{bitflags, Flags};
use fnv_rs::{Fnv64, FnvHasher};
use prettytable::Table;
use std::{
    collections::BTreeMap,
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

    pages: BTreeMap<Pgid, Page>,
    page_datas: BTreeMap<Pgid, Vec<u8>>,
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

trait ByteValue {}

impl ByteValue for u16 {}
impl ByteValue for u32 {}
impl ByteValue for u64 {}

fn read_value<T: ByteValue>(data: &Vec<u8>, offset: usize) -> T {
    let ptr: *const u8 = data.as_ptr();
    unsafe {
        let offset_ptr = ptr.offset(offset as isize) as *const T;
        return offset_ptr.read_unaligned();
    }
}

impl TryFrom<&Vec<u8>> for Page {
    type Error = String;

    fn try_from(data: &Vec<u8>) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(format!("Two small data {} to convert to Page", data.len()));
        }

        Ok(Page {
            id: Pgid(read_value::<u64>(data, 0)),
            flags: PageFlag::from_bits_truncate(read_value::<u16>(data, 8)),
            count: read_value::<u16>(data, 10),
            overflow: read_value::<u32>(data, 12),
        })
    }
}

#[derive(Debug, PartialEq, PartialOrd)]
#[repr(transparent)]
#[derive(Clone, Copy)]
struct Pgid(u64);

impl From<u64> for Pgid {
    fn from(id: u64) -> Self {
        Pgid(id)
    }
}

impl Into<u64> for Pgid {
    fn into(self) -> u64 {
        self.0
    }
}

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

impl TryFrom<&Vec<u8>> for Meta {
    type Error = String;

    fn try_from(data: &Vec<u8>) -> Result<Self, Self::Error> {
        if data.len() < 80 {
            return Err(format!("Two small data {} to convert to Meta", data.len()));
        }

        Ok(Meta {
            magic: read_value::<u32>(data, 16),
            version: read_value::<u32>(data, 20),
            page_size: read_value::<u32>(data, 24),
            _flag: 0,
            root_pgid: Pgid(read_value::<u64>(data, 32)),
            root_sequence: read_value::<u64>(data, 40),
            freelist_pgid: Pgid(read_value::<u64>(data, 48)),
            max_pgid: Pgid(read_value::<u64>(data, 56)),
            txid: read_value::<u64>(data, 64),
            checksum: read_value::<u64>(data, 72),
        })
    }
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

impl TryFrom<&Vec<u8>> for BranchPageElement {
    type Error = String;

    fn try_from(data: &Vec<u8>) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(format!(
                "Two small data {} to convert to BranchPageElement",
                data.len()
            ));
        }

        Ok(BranchPageElement {
            pos: read_value::<u32>(data, 0),
            ksize: read_value::<u32>(data, 4),
            pgid: Pgid(read_value::<u64>(data, 8)),
        })
    }
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

impl TryFrom<&Vec<u8>> for LeafPageElement {
    type Error = String;

    fn try_from(data: &Vec<u8>) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(format!(
                "Two small data {} to convert to LeafPageElement",
                data.len()
            ));
        }

        Ok(LeafPageElement {
            flags: read_value::<u32>(data, 0),
            pos: read_value::<u32>(data, 4),
            ksize: read_value::<u32>(data, 8),
            vsize: read_value::<u32>(data, 12),
        })
    }
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

impl TryFrom<&Vec<u8>> for Bucket {
    type Error = String;

    fn try_from(data: &Vec<u8>) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(format!(
                "Two small data {} to convert to Bucket",
                data.len()
            ));
        }

        Ok(Bucket {
            root: Pgid(read_value::<u64>(data, 0)),
            sequence: read_value::<u64>(data, 8),
        })
    }
}

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
            let offset_ptr = ptr.offset(offset as isize) as *const u64;
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
            let start = (16 + i * 16) as usize;
            let leaf_element: LeafPageElement =
                LeafPageElement::try_from(&page.get(start..page.len()).unwrap().to_vec()).unwrap();

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
                    let page: Page = TryFrom::try_from(&data).unwrap();
                    println!(
                        "bucket_page_id: {}, page_count: {}",
                        bucket_page_id, page.count
                    );
                    self.read_leaf_element(&data, page.count);
                    continue;
                }

                self.print_page(bucket_page_id);
            }
        }
    }

    fn read_branch_element(&mut self, page: &Vec<u8>, count: u16) {
        for i in 0..count {
            let next_page_id = read_value::<u64>(page, (16 + i * 16 + 8) as usize);
            self.print_page(next_page_id);
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

    pub fn print_page(&mut self, page_id: u64) {
        let data = self.read_page_overflow(page_id, 0);
        let page: Page = TryFrom::try_from(&data).unwrap();
        println!("print page: {:?}", page);

        let data = self.read_page_overflow(page_id, page.overflow);
        if page.flags.as_u16() == 0x02 {
            // leaf page
            self.read_leaf_element(&data, page.count);
        } else if page.flags.as_u16() == 0x01 {
            // branch page
            self.read_branch_element(&data, page.count);
        }
    }

    pub fn print_db(&mut self) {
        let data = self.read_page_overflow(0, 0);
        let page0: Page = TryFrom::try_from(&data).unwrap();
        println!("first page: {:?}", page0);
        if page0.flags.as_u16() != 0x04 {
            panic!("Invalid page 0's type")
        }

        let new_checksum =
            u64::from_be_bytes(Fnv64::hash(&data[16..72]).as_bytes().try_into().unwrap());
        let meta0: Meta = TryFrom::try_from(&data).unwrap();
        println!("first meta: {:?}", meta0);
        if meta0.checksum != new_checksum {
            panic!(
                "Invalid page 0's checksum, {:0x} != {:0x}",
                meta0.checksum, new_checksum
            );
        }

        let data = self.read_page_overflow(1, 0);
        let page1: Page = TryFrom::try_from(&data).unwrap();
        println!("second page: {:?}", page1);
        if page1.flags.as_u16() != 0x04 {
            panic!("Invalid page 0's type")
        }

        let meta1: Meta = TryFrom::try_from(&data).unwrap();
        println!("second meta: {:?}", meta1);
        let (page, meta) = if meta1.txid > meta0.txid {
            (page1, meta1)
        } else {
            (page0, meta0)
        };

        println!("Active root page: {:?} {:?}", page, meta);
        let data = self.read_page_overflow(meta.freelist_pgid.into(), 0);
        let freelist_page: Page = TryFrom::try_from(&data).unwrap();
        if !freelist_page.flags.contains(PageFlag::FreelistPageFlag) {
            panic!("Invalid freelist page type")
        }
        if freelist_page.count >= 0xFFFF {
            panic!("Too large page count")
        }

        let data = self.read_page_overflow(meta.freelist_pgid.into(), freelist_page.overflow);
        let freelist = self.read_freelist(&data, page.count);
        println!("Freelist: {:?}", freelist);

        let data = self.read_page_overflow(meta.root_pgid.into(), 0);
        let root_page: Page = TryFrom::try_from(&data).unwrap();
        if root_page.flags.as_u16() != 0x02 {
            panic!("Invalid root page's type")
        }

        self.print_page(root_page.id.into());

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
