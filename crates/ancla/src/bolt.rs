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

use crate::errors;
use crate::utils;
#[cfg(feature = "binrw")]
use binrw::BinRead;
use bitflags::bitflags;

#[derive(Debug, Clone, Copy)]
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

pub(crate) const PAGE_HEADER_SIZE: usize = std::mem::size_of::<Page>();

impl Page {
    fn decode(data: &[u8]) -> Self {
        Page {
            id: Pgid(utils::read_value::<u64>(data, 0)),
            flags: PageFlag::from_bits_truncate(utils::read_value::<u16>(data, 8)),
            count: utils::read_value::<u16>(data, 10),
            overflow: utils::read_value::<u32>(data, 12),
        }
    }
}

impl TryFrom<&[u8]> for Page {
    type Error = errors::DatabaseError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(errors::DatabaseError::TooSmallData {
                expect: 16,
                got: data.len(),
            });
        }

        Ok(Self::decode(data))
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq)]
#[cfg_attr(feature = "binrw", derive(binrw::BinRead))]
#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct Pgid(pub(crate) u64);

impl From<u64> for Pgid {
    fn from(id: u64) -> Self {
        Pgid(id)
    }
}

impl From<Pgid> for u64 {
    fn from(id: Pgid) -> u64 {
        id.0
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
        self.bits()
    }
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "binrw", derive(binrw::BinRead))]
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

impl Meta {
    #[cfg(not(feature = "binrw"))]
    fn decode(data: &[u8]) -> Self {
        Meta {
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
        }
    }

    #[cfg(feature = "binrw")]
    fn decode(data: &[u8]) -> Self {
        let mut cursor = std::io::Cursor::new(data.get(16..80).unwrap());
        let mut options = binrw::ReadOptions::default();
        options.endian = binrw::Endian::Little;
        options.offset = 0;
        Self::read_options(&mut cursor, &options, ()).unwrap()
    }
}

impl TryFrom<&[u8]> for Meta {
    type Error = errors::DatabaseError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 80 {
            return Err(errors::DatabaseError::TooSmallData {
                expect: 80,
                got: data.len(),
            });
        }

        Ok(Self::decode(data))
    }
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "binrw", derive(binrw::BinRead))]
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

impl BranchPageElement {
    #[cfg(not(feature = "binrw"))]
    fn decode(data: &[u8]) -> Self {
        BranchPageElement {
            pos: utils::read_value::<u32>(data, 0),
            ksize: utils::read_value::<u32>(data, 4),
            pgid: Pgid(utils::read_value::<u64>(data, 8)),
        }
    }

    #[cfg(feature = "binrw")]
    fn decode(data: &[u8]) -> Self {
        let mut cursor = std::io::Cursor::new(data);
        let mut options = binrw::ReadOptions::default();
        options.endian = binrw::Endian::Little;
        options.offset = 0;
        Self::read_options(&mut cursor, &options, ()).unwrap()
    }
}

impl TryFrom<&[u8]> for BranchPageElement {
    type Error = errors::DatabaseError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(errors::DatabaseError::TooSmallData {
                expect: 16,
                got: data.len(),
            });
        }

        Ok(Self::decode(data))
    }
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "binrw", derive(binrw::BinRead))]
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

impl LeafPageElement {
    #[cfg(not(feature = "binrw"))]
    fn decode(data: &[u8]) -> Self {
        LeafPageElement {
            flags: utils::read_value::<u32>(data, 0),
            pos: utils::read_value::<u32>(data, 4),
            ksize: utils::read_value::<u32>(data, 8),
            vsize: utils::read_value::<u32>(data, 12),
        }
    }

    #[cfg(feature = "binrw")]
    fn decode(data: &[u8]) -> Self {
        let mut cursor = std::io::Cursor::new(data);
        let mut options = binrw::ReadOptions::default();
        options.endian = binrw::Endian::Little;
        options.offset = 0;
        Self::read_options(&mut cursor, &options, ()).unwrap()
    }
}

impl TryFrom<&[u8]> for LeafPageElement {
    type Error = errors::DatabaseError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(errors::DatabaseError::TooSmallData {
                expect: 16,
                got: data.len(),
            });
        }

        Ok(Self::decode(data))
    }
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "binrw", derive(binrw::BinRead))]
#[repr(C)]
// Bucket represents the on-file representation of a bucket. It is stored as
// the `value` of a bucket key. If the root is 0, this bucket is small enough
// then it's root page can be stored inline in the value, just after the bucket header.
pub(crate) struct Bucket {
    // the bucket's root-level page.
    pub(crate) root: Pgid,
    sequence: u64,
}

impl Bucket {
    #[cfg(not(feature = "binrw"))]
    fn decode(data: &[u8]) -> Self {
        Bucket {
            root: Pgid(utils::read_value::<u64>(data, 0)),
            sequence: utils::read_value::<u64>(data, 8),
        }
    }

    #[cfg(feature = "binrw")]
    fn decode(data: &[u8]) -> Self {
        let mut cursor = std::io::Cursor::new(data);
        let mut options = binrw::ReadOptions::default();
        options.endian = binrw::Endian::Little;
        options.offset = 0;
        Self::read_options(&mut cursor, &options, ()).unwrap()
    }
}

impl TryFrom<&[u8]> for Bucket {
    type Error = errors::DatabaseError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(errors::DatabaseError::TooSmallData {
                expect: 16,
                got: data.len(),
            });
        }

        Ok(Self::decode(data))
    }
}

// Represents a marker value to indicate that a file is a Bolt DB.
pub(crate) const MAGIC_NUMBER: u32 = 0xED0CDAED;

// The data file format version.
pub(crate) const DATAFILE_VERSION: u32 = 2;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_try_from() {
        let data: [u8; 16] = [
            1, 0, 0, 0, 0, 0, 0, 0, // id
            1, 0, // flags
            0, 0, // count
            1, 0, 0, 0, // overflow
        ];
        let page = Page::try_from(&data as &[u8]).unwrap();
        assert_eq!(page.id.0, 1);
        assert_eq!(page.flags.as_u16(), 1);
        assert_eq!(page.count, 0);
        assert_eq!(page.overflow, 1);
    }

    #[test]
    fn test_page_try_from_too_small() {
        let data: [u8; 15] = [
            1, 0, 0, 0, 0, 0, 0, 0, // id
            1, 0, // flags
            0, 0, // count
            1, 0, 0, // overflow
        ];
        let result = Page::try_from(&data as &[u8]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            errors::DatabaseError::TooSmallData {
                expect: 16,
                got: 15
            }
        );
    }

    #[test]
    fn test_meta_try_from() {
        let data: [u8; 80] = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // PageHeader
            0xED, 0xDA, 0x0C, 0xED, // magic
            2, 0, 0, 0, // version
            1, 0, 0, 0, // page_size
            0, 0, 0, 0, // _flag
            1, 0, 0, 0, 0, 0, 0, 0, // root_pgid
            1, 0, 0, 0, 0, 0, 0, 0, // root_sequence
            1, 0, 0, 0, 0, 0, 0, 0, // freelist_pgid
            1, 0, 0, 0, 0, 0, 0, 0, // max_pgid
            1, 0, 0, 0, 0, 0, 0, 0, // txid
            b'a', 0, 0, 0, 0, 0, 0, 0, // checksum
        ];
        let meta = Meta::try_from(&data as &[u8]).unwrap();
        assert_eq!(meta.magic, MAGIC_NUMBER);
        assert_eq!(meta.version, DATAFILE_VERSION);
        assert_eq!(meta.page_size, 1);
        assert_eq!(meta.root_pgid, Pgid(1));
        assert_eq!(meta.root_sequence, 1);
        assert_eq!(meta.freelist_pgid, Pgid(1));
        assert_eq!(meta.max_pgid, Pgid(1));
        assert_eq!(meta.txid, 1);
        assert_eq!(meta.checksum, 97);
    }

    #[test]
    fn test_meta_try_from_too_small() {
        let data: [u8; 79] = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // PageHeader
            0xED, 0xDA, 0x0C, 0xED, // magic
            2, 0, 0, 0, // version
            1, 0, 0, 0, // page_size
            0, 0, 0, 0, // _flag
            1, 0, 0, 0, 0, 0, 0, 0, // root_pgid
            1, 0, 0, 0, 0, 0, 0, 0, // root_sequence
            1, 0, 0, 0, 0, 0, 0, 0, // freelist_pgid
            1, 0, 0, 0, 0, 0, 0, 0, // max_pgid
            1, 0, 0, 0, 0, 0, 0, 0, // txid
            b'a', 0, 0, 0, 0, 0, 0, // checksum
        ];
        let result = Meta::try_from(&data as &[u8]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            errors::DatabaseError::TooSmallData {
                expect: 80,
                got: 79
            }
        );
    }

    #[test]
    fn test_branch_page_element_try_from() {
        let data: [u8; 16] = [
            1, 0, 0, 0, // pos
            2, 0, 0, 0, // ksize
            1, 0, 0, 0, 0, 0, 0, 0, // pgid
        ];
        let element = BranchPageElement::try_from(&data as &[u8]).unwrap();
        assert_eq!(element.pos, 1);
        assert_eq!(element.ksize, 2);
        assert_eq!(element.pgid.0, 1);
    }

    #[test]
    fn test_branch_page_element_try_from_too_small() {
        let data: [u8; 15] = [
            1, 0, 0, 0, // pos
            2, 0, 0, 0, // ksize
            1, 0, 0, 0, 0, 0, 0, // pgid
        ];
        let result = BranchPageElement::try_from(&data as &[u8]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            errors::DatabaseError::TooSmallData {
                expect: 16,
                got: 15
            }
        );
    }

    #[test]
    fn test_leaf_page_element_try_from() {
        let data: [u8; 16] = [
            1, 0, 0, 0, // flags
            2, 0, 0, 0, // pos
            3, 0, 0, 0, // ksize
            4, 0, 0, 0, // vsize
        ];
        let element = LeafPageElement::try_from(&data as &[u8]).unwrap();
        assert_eq!(element.flags, 1);
        assert_eq!(element.pos, 2);
        assert_eq!(element.ksize, 3);
        assert_eq!(element.vsize, 4);
    }

    #[test]
    fn test_leaf_page_element_try_from_too_small() {
        let data: [u8; 15] = [
            1, 0, 0, 0, // flags
            2, 0, 0, 0, // pos
            3, 0, 0, 0, // ksize
            4, 0, 0, // vsize
        ];
        let result = LeafPageElement::try_from(&data as &[u8]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            errors::DatabaseError::TooSmallData {
                expect: 16,
                got: 15
            }
        );
    }

    #[test]
    fn test_bucket_try_from() {
        let data: [u8; 16] = [
            1, 0, 0, 0, 0, 0, 0, 0, // root
            2, 0, 0, 0, 0, 0, 0, 0, // sequence
        ];
        let bucket = Bucket::try_from(&data as &[u8]).unwrap();
        assert_eq!(bucket.root.0, 1);
        assert_eq!(bucket.sequence, 2);
    }

    #[test]
    fn test_bucket_try_from_too_small() {
        let data: [u8; 15] = [
            1, 0, 0, 0, 0, 0, 0, 0, // root
            2, 0, 0, 0, 0, 0, 0, // sequence
        ];
        let result = Bucket::try_from(&data as &[u8]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            errors::DatabaseError::TooSmallData {
                expect: 16,
                got: 15
            }
        );
    }
}
