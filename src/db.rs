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
    // id is the identifier of the page, it start from 0,
    // and is incremented by 1 for each page.
    // There have two special pages:
    //   - 0: meta0
    //   - 1: meta1
    // The are the root page of database, and the meta (valid) which have bigger
    // txid is current available.
    id: Pgid,
    flags: u16,
    count: u16,
    overflow: u32,
}

#[derive(Debug)]
#[repr(transparent)]
struct Pgid(u64);

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

    fn read_page_flag(&mut self, page: &Vec<u8>) -> u16 {
        let ptr: *const u8 = page.as_ptr();
        unsafe {
            let offset_ptr = ptr.offset(8) as *const u8;
            let value_ptr = std::slice::from_raw_parts(offset_ptr, 2);
            let value: u16 = u16::from_le_bytes(value_ptr.try_into().unwrap());
            value
        }
    }

    fn read_page_size(&mut self, page: &Vec<u8>) -> u32 {
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
            }
        }
    }

    pub fn build(ancla_options: AnclaOptions) -> DB {
        let file = File::open(ancla_options.db_path.clone()).unwrap();
        DB {
            options: ancla_options,
            file,
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
            self.read_page_size(&data)
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
            self.read_page_size(&data)
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

        let count = self.read_page_count(&data);
        self.read_leaf_element(&data, count);

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
