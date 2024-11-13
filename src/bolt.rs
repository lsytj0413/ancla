use crate::{errors, utils};
use bitflags::bitflags;

#[derive(Debug)]
#[repr(C)]
pub(crate) struct Page {
    // is the identifier of the page, it start from 0,
    // and is incremented by 1 for each page.
    // There have two special pages:
    //   - 0: meta0
    //   - 1: meta1
    // The are the root page of database, and the meta (valid) which have bigger
    // txid is current available.
    pub(crate) id: Pgid,
    // indicate which type this page is.
    pub(crate) flags: PageFlag,
    // number of element in this page, if the page is freelist page:
    // 1. if value < 0xFFFF, it's the number of pageid
    // 2. if value is 0xFFFF, the next 8-bytes（page's offset 16） is the number of pageid.
    pub(crate) count: u16,
    // the continous number of page, all page's data is stored in the buffer which
    // size is (1 + overflow) * PAGE_SIZE.
    pub(crate) overflow: u32,
}

impl TryFrom<&Vec<u8>> for Page {
    type Error = errors::DatabaseError;

    fn try_from(data: &Vec<u8>) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(errors::DatabaseError::TooSmallData {
                expect: 16,
                got: data.len(),
            });
        }

        Ok(Page {
            id: Pgid(utils::read_value::<u64>(data, 0)),
            flags: PageFlag::from_bits_truncate(utils::read_value::<u16>(data, 8)),
            count: utils::read_value::<u16>(data, 10),
            overflow: utils::read_value::<u32>(data, 12),
        })
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq)]
#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct Pgid(pub(crate) u64);

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
    pub(crate) struct PageFlag: u16 {
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
pub(crate) struct Meta {
    // The magic number of bolt database, must be MAGIC_NUMBER.
    pub(crate) magic: u32,
    // Database file format version, must be DATAFILE_VERSION.
    pub(crate) version: u32,
    // Size in bytes of each page.
    pub(crate) page_size: u32,
    _flag: u32, // unused
    // Rust doesn't have `type embedding` that Go has, see
    // https://github.com/rust-lang/rfcs/issues/2431 for more detail.
    // The root data pageid of the database.
    pub(crate) root_pgid: Pgid,
    pub(crate) root_sequence: u64,
    // The root freelist pageid of the database.
    pub(crate) freelist_pgid: Pgid,
    // The max pageid of the database, it shoule be FILE_SIZE / PAGE_SIZE.
    pub(crate) max_pgid: Pgid,
    // current max txid of the databse, there have two Meta page, which have bigger txid
    // is valid.
    pub(crate) txid: u64,
    pub(crate) checksum: u64,
}

impl TryFrom<&Vec<u8>> for Meta {
    type Error = errors::DatabaseError;

    fn try_from(data: &Vec<u8>) -> Result<Self, Self::Error> {
        if data.len() < 80 {
            return Err(errors::DatabaseError::TooSmallData {
                expect: 80,
                got: data.len(),
            });
        }

        Ok(Meta {
            magic: utils::read_value::<u32>(data, 16),
            version: utils::read_value::<u32>(data, 20),
            page_size: utils::read_value::<u32>(data, 24),
            _flag: 0,
            root_pgid: Pgid(utils::read_value::<u64>(data, 32)),
            root_sequence: utils::read_value::<u64>(data, 40),
            freelist_pgid: Pgid(utils::read_value::<u64>(data, 48)),
            max_pgid: Pgid(utils::read_value::<u64>(data, 56)),
            txid: utils::read_value::<u64>(data, 64),
            checksum: utils::read_value::<u64>(data, 72),
        })
    }
}

#[derive(Debug)]
#[repr(C)]
pub(crate) struct BranchPageElement {
    // pos is the offset of the element's data in the page,
    // start at current element's position.
    pub(crate) pos: u32,
    // the key's length in bytes.
    pub(crate) ksize: u32,
    // the next-level pageid.
    pub(crate) pgid: Pgid,
}

impl TryFrom<&Vec<u8>> for BranchPageElement {
    type Error = errors::DatabaseError;

    fn try_from(data: &Vec<u8>) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(errors::DatabaseError::TooSmallData {
                expect: 16,
                got: data.len(),
            });
        }

        Ok(BranchPageElement {
            pos: utils::read_value::<u32>(data, 0),
            ksize: utils::read_value::<u32>(data, 4),
            pgid: Pgid(utils::read_value::<u64>(data, 8)),
        })
    }
}

#[derive(Debug)]
#[repr(C)]
pub(crate) struct LeafPageElement {
    // indicate what type of the element, if flags is 1, it's a bucket,
    // otherwise it's a key-value pair.
    pub(crate) flags: u32,
    // pos is the offset of the element's data in the page,
    // start at current element's position.
    pub(crate) pos: u32,
    // the key's length in bytes.
    pub(crate) ksize: u32,
    // the value's length in bytes.
    pub(crate) vsize: u32,
}

impl TryFrom<&Vec<u8>> for LeafPageElement {
    type Error = errors::DatabaseError;

    fn try_from(data: &Vec<u8>) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(errors::DatabaseError::TooSmallData {
                expect: 16,
                got: data.len(),
            });
        }

        Ok(LeafPageElement {
            flags: utils::read_value::<u32>(data, 0),
            pos: utils::read_value::<u32>(data, 4),
            ksize: utils::read_value::<u32>(data, 8),
            vsize: utils::read_value::<u32>(data, 12),
        })
    }
}

#[derive(Debug)]
#[repr(C)]
// Bucket represents the on-file representation of a bucket. It is stored as
// the `value` of a bucket key. If the root is 0, this bucket is small enough
// then it's root page can be stored inline in the value, just after the bucket header.
pub(crate) struct Bucket {
    // the bucket's root-level page.
    pub(crate) root: Pgid,
    sequence: u64,
}

impl TryFrom<&Vec<u8>> for Bucket {
    type Error = errors::DatabaseError;

    fn try_from(data: &Vec<u8>) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(errors::DatabaseError::TooSmallData {
                expect: 16,
                got: data.len(),
            });
        }

        Ok(Bucket {
            root: Pgid(utils::read_value::<u64>(data, 0)),
            sequence: utils::read_value::<u64>(data, 8),
        })
    }
}
