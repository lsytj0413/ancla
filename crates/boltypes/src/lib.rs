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

use core::fmt;

#[cfg(feature = "binrw")]
use binrw::BinRead;
use bitflags::bitflags;
use fnv_rs::{Fnv64, FnvHasher};
use thiserror::Error;

#[derive(Error, Debug, Eq, PartialEq, Clone)]
pub enum Error {
    #[error("data buffer is too small, expect {expect}, got {got}")]
    TooSmallData { expect: usize, got: usize },

    #[error("expect kv leaf element but got an bucket")]
    UnexpectBucketLeaf,

    #[error("page {id} checksum is invalid, expect {expect}, got {got}")]
    InvalidPageChecksum { expect: u64, got: u64, id: u64 },

    #[error("page {id} magic is invalid, expect {expect}, got {got}")]
    InvalidPageMagic { expect: u32, got: u32, id: u64 },

    #[error("page {id} version is invalid, expect {expect}, got {got}")]
    InvalidPageVersion { expect: u32, got: u32, id: u64 },

    #[error("invalid data: {0}")]
    InvalidData(&'static str),
}

mod utils {
    trait ByteReadMarker {}

    impl ByteReadMarker for u16 {}
    impl ByteReadMarker for u32 {}
    impl ByteReadMarker for u64 {}

    #[allow(private_bounds)]
    pub(crate) fn read_value<T: ByteReadMarker>(data: &[u8], offset: usize) -> T {
        assert!(
            (data.len() - offset) >= std::mem::size_of::<T>(),
            "data didn't have enough length: expect atleast {}, got {}",
            std::mem::size_of::<T>(),
            (data.len() - offset)
        );

        let ptr: *const u8 = data.as_ptr();
        unsafe {
            let offset_ptr = ptr.add(offset) as *const T;
            offset_ptr.read_unaligned()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_read_value_u64_success() {
            let data: [u8; 8] = [1, 0, 0, 0, 0, 0, 0, 0];
            assert_eq!(read_value::<u64>(&data, 0), 1);
        }

        #[test]
        #[should_panic(expected = "expect atleast 8, got 7")]
        fn test_read_value_not_enough_data() {
            let data: [u8; 7] = [1, 0, 0, 0, 0, 0, 0];
            read_value::<u64>(&data, 0);
        }
    }
}

/// PageHeader is the bolt's page metadata definition, every page must have this definition
/// at it's start (offset 0), it defines the type of page and how to parse it etc.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "binrw", derive(binrw::BinRead))]
#[repr(C)]
pub struct PageHeader {
    /// id is the identifier of the page, it start from 0,
    /// and is incremented by 1 for each page.
    /// There have two special pages:
    ///   - 0: meta0
    ///   - 1: meta1
    ///
    /// The are the root page of database, and the meta (valid) which have bigger
    /// txid is current available.
    pub id: Pgid,

    /// indicate which type this page is.
    #[cfg_attr(feature = "binrw", br(parse_with = pageflag_custom_parse))]
    pub flags: PageFlag,

    /// number of element in this page, if the page is freelist page:
    /// 1. when value < 0xFFFF, it's the number of pageid
    /// 2. when value is 0xFFFF, the next 8-bytes（page's offset 16） is the number of pageid.
    pub count: u16,

    /// the continous number of page, all page's data is stored in the buffer which
    /// size is (1 + overflow) * PAGE_SIZE.
    pub overflow: u32,
}

pub const PAGE_HEADER_SIZE: usize = std::mem::size_of::<PageHeader>();

impl PageHeader {
    #[cfg(feature = "binrw")]
    fn decode(data: &[u8]) -> Result<Self, Error> {
        let mut cursor = std::io::Cursor::new(data);
        let mut options = binrw::ReadOptions::default();
        options.endian = binrw::Endian::Little;
        options.offset = 0;
        Self::read_options(&mut cursor, &options, ())
            .map_err(|_| Error::InvalidData("failed to parse PageHeader"))
    }

    #[cfg(not(feature = "binrw"))]
    fn decode(data: &[u8]) -> Self {
        PageHeader {
            id: Pgid(utils::read_value::<u64>(data, 0)),
            flags: PageFlag::from_bits_truncate(utils::read_value::<u16>(data, 8)),
            count: utils::read_value::<u16>(data, 10),
            overflow: utils::read_value::<u32>(data, 12),
        }
    }
}

impl TryFrom<&[u8]> for PageHeader {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(Error::TooSmallData {
                expect: 16,
                got: data.len(),
            });
        }

        #[cfg(feature = "binrw")]
        {
            Self::decode(data)
        }
        #[cfg(not(feature = "binrw"))]
        {
            Ok(Self::decode(data))
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "binrw", derive(binrw::BinRead))]
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct Pgid(pub u64);

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

impl std::fmt::Display for Pgid {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct PageFlag: u16 {
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

    pub fn is_branch_page(&self) -> bool {
        self.contains(PageFlag::BranchPageFlag)
    }

    pub fn is_meta_page(&self) -> bool {
        self.contains(PageFlag::MetaPageFlag)
    }

    pub fn is_leaf_page(&self) -> bool {
        self.contains(PageFlag::LeafPageFlag)
    }

    pub fn is_freelist_page(&self) -> bool {
        self.contains(PageFlag::FreelistPageFlag)
    }
}

impl std::fmt::Display for PageFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(feature = "binrw")]
fn pageflag_custom_parse<R: binrw::io::Read + binrw::io::Seek>(
    reader: &mut R,
    _ro: &binrw::ReadOptions,
    _: (),
) -> binrw::BinResult<PageFlag> {
    let mut buf = [0; 2];
    reader.read_exact(&mut buf)?;
    Ok(PageFlag::from_bits_truncate(utils::read_value::<u16>(
        &buf, 0,
    )))
}

#[derive(Debug, Clone)]
pub enum Page {
    MetaPage(MetaPage),
    FreelistPage(FreelistPage),
    BranchPage(BranchPage),
    LeafPage(LeafPage),
}

impl Page {
    pub fn new(data: Vec<u8>, page_size: usize) -> Result<Page, Error> {
        if page_size == 0 {
            return Err(Error::InvalidData("page size cannot be zero"));
        }
        let header: PageHeader = TryFrom::try_from(data.as_slice())?;

        let page = if header.flags.is_meta_page() {
            Page::MetaPage(MetaPage::new(data, page_size)?)
        } else if header.flags.is_freelist_page() {
            Page::FreelistPage(FreelistPage::new(data, page_size)?)
        } else if header.flags.is_branch_page() {
            Page::BranchPage(BranchPage::new(data, page_size)?)
        } else if header.flags.is_leaf_page() {
            Page::LeafPage(LeafPage::new(data, page_size)?)
        } else {
            return Err(Error::InvalidData("unknown page flags"));
        };

        Ok(page)
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Page::MetaPage(meta) => meta.data.as_slice(),
            Page::FreelistPage(freelist) => freelist.data.as_slice(),
            Page::BranchPage(branch) => branch.data.as_slice(),
            Page::LeafPage(leaf) => leaf.data.as_slice(),
        }
    }

    pub fn page_header(&self) -> PageHeader {
        match self {
            Page::MetaPage(meta) => meta.page_header(),
            Page::FreelistPage(freelist) => freelist.page_header(),
            Page::BranchPage(branch) => branch.page_header(),
            Page::LeafPage(leaf) => leaf.page_header(),
        }
    }

    pub fn used(&self) -> usize {
        match self {
            Page::MetaPage(meta) => meta.used(),
            Page::FreelistPage(freelist) => freelist.used(),
            Page::BranchPage(branch) => branch.used(),
            Page::LeafPage(leaf) => leaf.used(),
        }
    }

    pub fn page_size(&self) -> usize {
        match self {
            Page::MetaPage(meta) => meta.page_size,
            Page::FreelistPage(freelist) => freelist.page_size,
            Page::BranchPage(branch) => branch.page_size,
            Page::LeafPage(leaf) => leaf.page_size,
        }
    }

    pub fn capacity(&self) -> usize {
        self.as_slice().len()
    }
}

#[derive(Debug, Clone)]
pub struct MetaPage {
    data: Vec<u8>,
    header: PageHeader,
    page_size: usize,
    used: usize,
}

impl MetaPage {
    pub fn new(data: Vec<u8>, page_size: usize) -> Result<Self, Error> {
        let header = PageHeader::try_from(data.as_slice())?;
        if page_size * (header.overflow as usize + 1) != data.len() {
            return Err(Error::InvalidData(
                "data size mismatch with page size and overflow",
            ));
        }
        let used = PAGE_HEADER_SIZE + std::mem::size_of::<Meta>();
        Ok(MetaPage {
            data,
            header,
            page_size,
            used,
        })
    }

    pub fn page_header(&self) -> PageHeader {
        self.header
    }

    pub fn used(&self) -> usize {
        self.used
    }

    pub fn meta(&self) -> Result<Meta, Error> {
        Meta::try_from(self.data.as_slice())
    }
}

#[derive(Debug, Clone)]
pub struct FreelistPage {
    data: Vec<u8>,
    header: PageHeader,
    page_size: usize,
    used: usize,
}

impl FreelistPage {
    pub fn new(data: Vec<u8>, page_size: usize) -> Result<Self, Error> {
        let header = PageHeader::try_from(data.as_slice())?;
        if page_size * (header.overflow as usize + 1) != data.len() {
            return Err(Error::InvalidData(
                "data size mismatch with page size and overflow",
            ));
        }
        let used = Self::calculate_used(&header, &data)?;
        Ok(FreelistPage {
            data,
            header,
            page_size,
            used,
        })
    }

    pub fn page_header(&self) -> PageHeader {
        self.header
    }

    pub fn used(&self) -> usize {
        self.used
    }

    fn calculate_used(header: &PageHeader, data: &[u8]) -> Result<usize, Error> {
        let (count, offset) = if header.count != 0xFFFF {
            (header.count as u64, 0)
        } else {
            (utils::read_value::<u64>(data, PAGE_HEADER_SIZE), 8)
        };
        Ok(PAGE_HEADER_SIZE + offset + (count as usize) * std::mem::size_of::<Pgid>())
    }

    pub fn free_pages(&self) -> Result<Vec<Pgid>, Error> {
        let header = self.page_header();
        assert!(
            header.flags.is_freelist_page(),
            "expect freelist page {} but got {}",
            header.id,
            header.flags
        );

        let (count, offset) = if header.count != 0xFF {
            (header.count as u64, 0)
        } else {
            (
                utils::read_value::<u64>(self.data.as_slice(), PAGE_HEADER_SIZE),
                8,
            )
        };

        let mut freelist: Vec<Pgid> = Vec::with_capacity(count as usize);
        for i in 0..count {
            freelist.push(Pgid::from(utils::read_value::<u64>(
                self.data.as_slice(),
                (i as usize) * 8 + PAGE_HEADER_SIZE + offset,
            )));
        }
        Ok(freelist)
    }
}

#[derive(Debug, Clone)]
pub struct BranchPage {
    data: Vec<u8>,
    header: PageHeader,
    page_size: usize,
    used: usize,
}

impl BranchPage {
    pub fn new(data: Vec<u8>, page_size: usize) -> Result<Self, Error> {
        let header = PageHeader::try_from(data.as_slice())?;
        if page_size * (header.overflow as usize + 1) != data.len() {
            return Err(Error::InvalidData(
                "data size mismatch with page size and overflow",
            ));
        }
        let used = Self::calculate_used(&header, &data)?;
        Ok(BranchPage {
            data,
            header,
            page_size,
            used,
        })
    }

    pub fn page_header(&self) -> PageHeader {
        self.header
    }

    pub fn used(&self) -> usize {
        self.used
    }

    fn calculate_used(header: &PageHeader, data: &[u8]) -> Result<usize, Error> {
        if header.count == 0 {
            return Ok(PAGE_HEADER_SIZE);
        }

        let last_element_idx = header.count - 1;
        let start = PAGE_HEADER_SIZE + (last_element_idx as usize) * BRANCH_ELEMENT_HEADER_SIZE;
        let elem_header: BranchElementHeader = TryFrom::try_from(data.get(start..).ok_or(
            Error::InvalidData("slice out of bounds for branch element header"),
        )?)?;
        Ok(start + elem_header.pos as usize + elem_header.ksize as usize)
    }

    pub fn branch_elements(&self) -> Result<Vec<BranchElement>, Error> {
        let header = self.page_header();
        assert!(
            header.flags.is_branch_page(),
            "expect branch page {} but got {}",
            header.id,
            header.flags
        );

        let mut elements: Vec<BranchElement> = Vec::with_capacity(header.count as usize);
        for i in 0..header.count {
            let start = PAGE_HEADER_SIZE + (i as usize) * BRANCH_ELEMENT_HEADER_SIZE;
            let elem_header: BranchElementHeader =
                TryFrom::try_from(self.data.get(start..).ok_or(Error::InvalidData(
                    "slice out of bounds for branch element header",
                ))?)?;
            elements.push(BranchElement::from_page(self, &elem_header, i)?);
        }

        Ok(elements)
    }
}

#[derive(Debug, Clone)]
pub struct LeafPage {
    data: Vec<u8>,
    header: PageHeader,
    page_size: usize,
    used: usize,
}

impl LeafPage {
    pub fn new(data: Vec<u8>, page_size: usize) -> Result<Self, Error> {
        let header = PageHeader::try_from(data.as_slice())?;
        if page_size * (header.overflow as usize + 1) != data.len() {
            return Err(Error::InvalidData(
                "data size mismatch with page size and overflow",
            ));
        }
        let used = Self::calculate_used(&header, &data)?;
        Ok(LeafPage {
            data,
            header,
            page_size,
            used,
        })
    }

    pub fn page_header(&self) -> PageHeader {
        self.header
    }

    pub fn used(&self) -> usize {
        self.used
    }

    fn calculate_used(header: &PageHeader, data: &[u8]) -> Result<usize, Error> {
        if header.count == 0 {
            return Ok(PAGE_HEADER_SIZE);
        }

        let last_element_idx = header.count - 1;
        let start = PAGE_HEADER_SIZE + (last_element_idx as usize) * LEAF_ELEMENT_HEADER_SIZE;
        let elem_header: LeafElementHeader = TryFrom::try_from(data.get(start..).ok_or(
            Error::InvalidData("slice out of bounds for leaf element header"),
        )?)?;
        Ok(start
            + elem_header.pos as usize
            + elem_header.ksize as usize
            + elem_header.vsize as usize)
    }

    pub fn leaf_elements(&self) -> Result<Vec<LeafElement>, Error> {
        let header = self.page_header();
        assert!(
            header.flags.is_leaf_page(),
            "expect leaf page {} but got {}",
            header.id,
            header.flags
        );

        let mut elements: Vec<LeafElement> = Vec::with_capacity(header.count as usize);
        for i in 0..header.count {
            let start = PAGE_HEADER_SIZE + (i as usize) * LEAF_ELEMENT_HEADER_SIZE;
            let elem_header: LeafElementHeader = TryFrom::try_from(self.data.get(start..).ok_or(
                Error::InvalidData("slice out of bounds for leaf element header"),
            )?)?;
            elements.push(LeafElement::from_page(self, &elem_header, i)?);
        }

        Ok(elements)
    }
}

/// Meta represent the definition of meta page's structure.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "binrw", derive(binrw::BinRead))]
#[repr(C)]
pub struct Meta {
    /// The magic number of bolt database, must be MAGIC_NUMBER.
    pub magic: u32,

    /// Database file format version, must be DATAFILE_VERSION.
    pub version: u32,

    /// Size in bytes of each page.
    pub page_size: u32,
    _flag: u32, // unused

    // Rust doesn't have `type embedding` that Go has, see
    // https://github.com/rust-lang/rfcs/issues/2431 for more detail.
    /// The root data pageid of the database.
    pub root_pgid: Pgid,
    pub root_sequence: u64,

    /// The root freelist pageid of the database.
    pub freelist_pgid: Pgid,

    /// The max pageid of the database, it shoule be FILE_SIZE / PAGE_SIZE.
    pub max_pgid: Pgid,

    /// current max txid of the databse, there have two Meta page, which have bigger txid
    /// is valid.
    pub txid: u64,
    pub checksum: u64,
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
    fn decode(data: &[u8]) -> Result<Self, Error> {
        let mut cursor = std::io::Cursor::new(data.get(16..80).ok_or(Error::TooSmallData {
            expect: 80,
            got: data.len(),
        })?);
        let mut options = binrw::ReadOptions::default();
        options.endian = binrw::Endian::Little;
        options.offset = 0;
        Self::read_options(&mut cursor, &options, ())
            .map_err(|_| Error::InvalidData("failed to parse Meta"))
    }
}

impl TryFrom<&[u8]> for Meta {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 80 {
            return Err(Error::TooSmallData {
                expect: 80,
                got: data.len(),
            });
        }

        #[cfg(feature = "binrw")]
        let meta = Self::decode(data)?;
        #[cfg(not(feature = "binrw"))]
        let meta = Self::decode(data);

        let page_header = PageHeader::try_from(data)?;
        if meta.magic != MAGIC_NUMBER {
            return Err(Error::InvalidPageMagic {
                expect: MAGIC_NUMBER,
                got: meta.magic,
                id: page_header.id.into(),
            });
        }
        if meta.version != DATAFILE_VERSION {
            return Err(Error::InvalidPageVersion {
                expect: DATAFILE_VERSION,
                got: meta.version,
                id: page_header.id.into(),
            });
        }

        let actual_checksum = u64::from_be_bytes(
            Fnv64::hash(&data[16..72])
                .as_bytes()
                .try_into()
                .map_err(|_| Error::InvalidData("calculate checksum failed"))?,
        );
        if meta.checksum != actual_checksum {
            return Err(Error::InvalidPageChecksum {
                expect: actual_checksum,
                got: meta.checksum,
                id: page_header.id.into(),
            });
        }

        Ok(meta)
    }
}

/// Represent the structure when page is branch.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "binrw", derive(binrw::BinRead))]
#[repr(C)]
pub struct BranchElementHeader {
    /// pos is the offset of the element's data in the page,
    /// start at current element's position.
    pub pos: u32,

    /// the key's length in bytes.
    pub ksize: u32,

    /// the next-level pageid.
    pub pgid: Pgid,
}

impl BranchElementHeader {
    #[cfg(not(feature = "binrw"))]
    fn decode(data: &[u8]) -> Self {
        BranchElementHeader {
            pos: utils::read_value::<u32>(data, 0),
            ksize: utils::read_value::<u32>(data, 4),
            pgid: Pgid(utils::read_value::<u64>(data, 8)),
        }
    }

    #[cfg(feature = "binrw")]
    fn decode(data: &[u8]) -> Result<Self, Error> {
        let mut cursor = std::io::Cursor::new(data);
        let mut options = binrw::ReadOptions::default();
        options.endian = binrw::Endian::Little;
        options.offset = 0;
        Self::read_options(&mut cursor, &options, ())
            .map_err(|_| Error::InvalidData("failed to parse BranchElementHeader"))
    }
}

impl TryFrom<&[u8]> for BranchElementHeader {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(Error::TooSmallData {
                expect: 16,
                got: data.len(),
            });
        }

        #[cfg(feature = "binrw")]
        {
            Self::decode(data)
        }
        #[cfg(not(feature = "binrw"))]
        {
            Ok(Self::decode(data))
        }
    }
}

pub const BRANCH_ELEMENT_HEADER_SIZE: usize = std::mem::size_of::<BranchElementHeader>();

/// Represents branch element in branch page.
#[derive(Debug, Clone)]
pub struct BranchElement {
    pub key: Vec<u8>,
    pub pgid: Pgid,
}

impl BranchElement {
    /// Returns the branch element in the page data.
    ///
    /// # Arguments
    ///
    /// * `page` - data of current page
    /// * `elem` - element header of current branch
    /// * `idx` - idx of current element header in this page, start from 0
    fn from_page(
        page: &BranchPage,
        elem: &BranchElementHeader,
        idx: u16,
    ) -> Result<BranchElement, Error> {
        let start = PAGE_HEADER_SIZE + (idx as usize) * BRANCH_ELEMENT_HEADER_SIZE;
        let key_start = start + elem.pos as usize;
        let key_end = key_start + elem.ksize as usize;

        if key_end > page.data.len() {
            return Err(Error::TooSmallData {
                expect: key_end,
                got: page.data.len(),
            });
        }

        Ok(BranchElement {
            key: page
                .data
                .get(key_start..key_end)
                .ok_or(Error::InvalidData("key slice out of bounds"))?
                .to_vec(),
            pgid: elem.pgid,
        })
    }
}

/// LeafElementHeader represent the element's structure when page is leaf.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "binrw", derive(binrw::BinRead))]
#[repr(C)]
pub struct LeafElementHeader {
    /// indicate what type of the element, if flags is 1, it's a bucket,
    /// otherwise it's a key-value pair.
    pub flags: u32,

    /// pos is the offset of the element's data in the page,
    /// start at current element's position.
    pub pos: u32,

    /// the key's length in bytes.
    pub ksize: u32,

    /// the value's length in bytes.
    pub vsize: u32,
}

impl LeafElementHeader {
    #[cfg(not(feature = "binrw"))]
    fn decode(data: &[u8]) -> Self {
        LeafElementHeader {
            flags: utils::read_value::<u32>(data, 0),
            pos: utils::read_value::<u32>(data, 4),
            ksize: utils::read_value::<u32>(data, 8),
            vsize: utils::read_value::<u32>(data, 12),
        }
    }

    #[cfg(feature = "binrw")]
    fn decode(data: &[u8]) -> Result<Self, Error> {
        let mut cursor = std::io::Cursor::new(data);
        let mut options = binrw::ReadOptions::default();
        options.endian = binrw::Endian::Little;
        options.offset = 0;
        Self::read_options(&mut cursor, &options, ())
            .map_err(|_| Error::InvalidData("failed to parse LeafElementHeader"))
    }

    pub fn is_bucket(&self) -> bool {
        self.flags == 0x01
    }
}

impl TryFrom<&[u8]> for LeafElementHeader {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(Error::TooSmallData {
                expect: 16,
                got: data.len(),
            });
        }

        #[cfg(feature = "binrw")]
        {
            Self::decode(data)
        }
        #[cfg(not(feature = "binrw"))]
        {
            Ok(Self::decode(data))
        }
    }
}

pub const LEAF_ELEMENT_HEADER_SIZE: usize = std::mem::size_of::<LeafElementHeader>();

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "binrw", derive(binrw::BinRead))]
#[repr(C)]
/// BucketHeader represents the on-file representation of a bucket key's value. It is stored as
/// the `value` of a bucket key. If the root is 0, this bucket is small enough
/// then it's root page can be stored inline in the value, just after the bucket header.
pub struct BucketHeader {
    /// the bucket's root-level page.
    pub root: Pgid,
    pub sequence: u64,
}

impl BucketHeader {
    #[cfg(not(feature = "binrw"))]
    fn decode(data: &[u8]) -> Self {
        BucketHeader {
            root: Pgid(utils::read_value::<u64>(data, 0)),
            sequence: utils::read_value::<u64>(data, 8),
        }
    }

    #[cfg(feature = "binrw")]
    fn decode(data: &[u8]) -> Result<Self, Error> {
        let mut cursor = std::io::Cursor::new(data);
        let mut options = binrw::ReadOptions::default();
        options.endian = binrw::Endian::Little;
        options.offset = 0;
        Self::read_options(&mut cursor, &options, ())
            .map_err(|_| Error::InvalidData("failed to parse BucketHeader"))
    }

    pub fn is_inline(self) -> bool {
        Into::<u64>::into(self.root) == 0
    }
}

impl TryFrom<&[u8]> for BucketHeader {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 16 {
            return Err(Error::TooSmallData {
                expect: 16,
                got: data.len(),
            });
        }

        #[cfg(feature = "binrw")]
        {
            Self::decode(data)
        }
        #[cfg(not(feature = "binrw"))]
        {
            Ok(Self::decode(data))
        }
    }
}

pub const BUCKET_HEADER_SIZE: usize = std::mem::size_of::<BucketHeader>();

/// Represents a marker value to indicate that a file is a Bolt DB.
pub const MAGIC_NUMBER: u32 = 0xED0CDAED;

/// The data file format version.
pub const DATAFILE_VERSION: u32 = 2;

/// Represents key and value element in leaf page.
#[derive(Clone, Debug, Default)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl KeyValue {
    /// Returns the key and value in the page data.
    ///
    /// # Arguments
    ///
    /// * `page` - data of current page
    /// * `elem` - leaf element header of current kv, it can't be a bucket
    /// * `idx` - idx of current leaf element header in this page, start from 0
    ///
    /// # Returns
    ///
    /// kv of current leaf element
    fn from_page(page: &[u8], elem: &LeafElementHeader, idx: u16) -> Result<KeyValue, Error> {
        if elem.is_bucket() {
            return Err(Error::UnexpectBucketLeaf);
        }

        let start = PAGE_HEADER_SIZE + (idx as usize) * LEAF_ELEMENT_HEADER_SIZE;
        let key_start = start + elem.pos as usize;
        let key_end = key_start + elem.ksize as usize;
        let value_end = key_end + elem.vsize as usize;

        if value_end > page.len() {
            return Err(Error::TooSmallData {
                expect: value_end,
                got: page.len(),
            });
        }

        Ok(KeyValue {
            key: page
                .get(key_start..key_end)
                .ok_or(Error::InvalidData("key slice out of bounds"))?
                .to_vec(),
            value: page
                .get(key_end..value_end)
                .ok_or(Error::InvalidData("value slice out of bounds"))?
                .to_vec(),
        })
    }
}

/// Represents an element stored in a leaf page.
///
/// A leaf page can contain regular key-value pairs, nested buckets,
/// or inline buckets (small buckets stored directly within the leaf page).
#[derive(Debug, Clone)]
pub enum LeafElement {
    /// Represents a nested bucket that is not stored inline.
    /// The actual bucket data is located on a separate page.
    Bucket {
        /// The name of the bucket.
        name: Vec<u8>,
        /// The page ID of the bucket's root page, where its own elements are stored.
        root_pgid: Pgid,
        /// The page ID of the leaf page where this bucket definition resides.
        pgid: Pgid,
    },
    /// Represents a small bucket whose data is stored directly within the parent leaf page.
    /// This avoids the overhead of allocating a separate page for the bucket.
    InlineBucket {
        /// The name of the inline bucket.
        name: Vec<u8>,
        /// The root page ID of the bucket. For inline buckets, this is always 0.
        root_pgid: Pgid,
        /// The page ID of the leaf page where this inline bucket is stored.
        pgid: Pgid,
        /// The key-value pairs stored directly within this inline bucket.
        items: Vec<KeyValue>,
    },
    /// Represents a standard key-value pair.
    KeyValue(KeyValue),
}

impl LeafElement {
    /// Returns the elem in the page data.
    ///
    /// # Arguments
    ///
    /// * `page` - data of current page
    /// * `elem` - leaf element header
    /// * `idx` - idx of current leaf element header in this page, start from 0
    ///
    /// # Returns
    ///
    /// current leaf element
    fn from_page(
        page: &LeafPage,
        elem: &LeafElementHeader,
        idx: u16,
    ) -> Result<LeafElement, Error> {
        if !elem.is_bucket() {
            return KeyValue::from_page(page.data.as_slice(), elem, idx).map(LeafElement::KeyValue);
        }

        let start = PAGE_HEADER_SIZE + (idx as usize) * LEAF_ELEMENT_HEADER_SIZE;
        let key_start = start + elem.pos as usize;
        let key_end = key_start + elem.ksize as usize;
        let value_end = key_end + elem.vsize as usize;

        if value_end > page.data.len() {
            return Err(Error::TooSmallData {
                expect: value_end,
                got: page.data.len(),
            });
        }

        let key = page
            .data
            .get(key_start..key_end)
            .ok_or(Error::InvalidData("key slice out of bounds"))?;
        let value = page
            .data
            .get(key_end..value_end)
            .ok_or(Error::InvalidData("value slice out of bounds"))?;

        let bucket_header: BucketHeader = TryFrom::try_from(value)?;
        if !bucket_header.is_inline() {
            return Ok(LeafElement::Bucket {
                name: key.to_vec(),
                root_pgid: bucket_header.root,
                pgid: page.page_header().id,
            });
        }

        let inline_page_data = value
            .get(BUCKET_HEADER_SIZE..)
            .ok_or(Error::InvalidData("inline page slice out of bounds"))?;
        let inline_page = LeafPage::new(inline_page_data.to_vec(), inline_page_data.len())?; // For inline pages, page_size is not meaningful
        Ok(LeafElement::InlineBucket {
            name: key.to_vec(),
            root_pgid: bucket_header.root,
            pgid: page.page_header().id,
            items: inline_page
                .leaf_elements()?
                .into_iter()
                .map(|x| match x {
                    LeafElement::KeyValue(kv) => Ok(kv),
                    _ => Err(Error::InvalidData(
                        "unreachable: non-kv element in inline bucket",
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test that a valid byte slice can be successfully converted into a PageHeader.
    // This ensures that the basic parsing of the page header from a byte slice is working correctly.
    #[test]
    fn test_page_try_from() {
        let mut data = [0; PAGE_HEADER_SIZE];
        data[0..8].copy_from_slice(&1u64.to_le_bytes());
        data[8..10].copy_from_slice(&PageFlag::BranchPageFlag.bits().to_le_bytes());
        data[10..12].copy_from_slice(&0u16.to_le_bytes());
        data[12..16].copy_from_slice(&1u32.to_le_bytes());

        let page = PageHeader::try_from(&data as &[u8]).unwrap();
        assert_eq!(page.id.0, 1);
        assert_eq!(page.flags, PageFlag::BranchPageFlag);
        assert_eq!(page.count, 0);
        assert_eq!(page.overflow, 1);
    }

    // Test that converting a byte slice that is too small results in an error.
    // This is important to ensure that the system handles corrupted or incomplete data gracefully.
    #[test]
    fn test_page_try_from_too_small() {
        let data: [u8; PAGE_HEADER_SIZE - 1] = [0; PAGE_HEADER_SIZE - 1];
        let result = PageHeader::try_from(&data as &[u8]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            Error::TooSmallData {
                expect: PAGE_HEADER_SIZE,
                got: PAGE_HEADER_SIZE - 1
            }
        );
    }

    // Test that a valid byte slice can be successfully converted into a Meta struct.
    // This test verifies that the metadata of the database is read correctly.
    #[test]
    fn test_meta_try_from() {
        let mut data = [0; 80];
        data[16..20].copy_from_slice(&MAGIC_NUMBER.to_le_bytes());
        data[20..24].copy_from_slice(&DATAFILE_VERSION.to_le_bytes());
        data[24..28].copy_from_slice(&1u32.to_le_bytes());
        data[32..40].copy_from_slice(&1u64.to_le_bytes());
        data[40..48].copy_from_slice(&1u64.to_le_bytes());
        data[48..56].copy_from_slice(&1u64.to_le_bytes());
        data[56..64].copy_from_slice(&1u64.to_le_bytes());
        data[64..72].copy_from_slice(&1u64.to_le_bytes());

        let checksum =
            u64::from_be_bytes(Fnv64::hash(&data[16..72]).as_bytes().try_into().unwrap());
        data[72..80].copy_from_slice(&checksum.to_le_bytes());

        let meta = Meta::try_from(&data as &[u8]).unwrap();
        assert_eq!(meta.magic, MAGIC_NUMBER);
        assert_eq!(meta.version, DATAFILE_VERSION);
        assert_eq!(meta.page_size, 1);
        assert_eq!(meta.root_pgid, Pgid(1));
        assert_eq!(meta.root_sequence, 1);
        assert_eq!(meta.freelist_pgid, Pgid(1));
        assert_eq!(meta.max_pgid, Pgid(1));
        assert_eq!(meta.txid, 1);
        assert_eq!(meta.checksum, checksum);
    }

    // Test that converting a byte slice that is too small for a Meta struct results in an error.
    // This ensures robustness against corrupted database files.
    #[test]
    fn test_meta_try_from_too_small() {
        let data: [u8; 79] = [0; 79];
        let result = Meta::try_from(&data as &[u8]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            Error::TooSmallData {
                expect: 80,
                got: 79
            }
        );
    }

    // Test that a valid byte slice can be converted into a BranchElementHeader.
    // This is crucial for navigating the B-tree structure of the database.
    #[test]
    fn test_branch_page_element_try_from() {
        let mut data = [0; BRANCH_ELEMENT_HEADER_SIZE];
        data[0..4].copy_from_slice(&1u32.to_le_bytes());
        data[4..8].copy_from_slice(&2u32.to_le_bytes());
        data[8..16].copy_from_slice(&1u64.to_le_bytes());

        let element = BranchElementHeader::try_from(&data as &[u8]).unwrap();
        assert_eq!(element.pos, 1);
        assert_eq!(element.ksize, 2);
        assert_eq!(element.pgid.0, 1);
    }

    // Test that converting a byte slice that is too small for a BranchElementHeader results in an error.
    // This helps prevent panics when reading malformed branch pages.
    #[test]
    fn test_branch_page_element_try_from_too_small() {
        let data: [u8; BRANCH_ELEMENT_HEADER_SIZE - 1] = [0; BRANCH_ELEMENT_HEADER_SIZE - 1];
        let result = BranchElementHeader::try_from(&data as &[u8]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            Error::TooSmallData {
                expect: BRANCH_ELEMENT_HEADER_SIZE,
                got: BRANCH_ELEMENT_HEADER_SIZE - 1
            }
        );
    }

    // Test that a valid byte slice can be converted into a LeafElementHeader.
    // This is essential for reading key/value pairs from leaf pages.
    #[test]
    fn test_leaf_page_element_try_from() {
        let mut data = [0; LEAF_ELEMENT_HEADER_SIZE];
        data[0..4].copy_from_slice(&1u32.to_le_bytes());
        data[4..8].copy_from_slice(&2u32.to_le_bytes());
        data[8..12].copy_from_slice(&3u32.to_le_bytes());
        data[12..16].copy_from_slice(&4u32.to_le_bytes());

        let element = LeafElementHeader::try_from(&data as &[u8]).unwrap();
        assert_eq!(element.flags, 1);
        assert_eq!(element.pos, 2);
        assert_eq!(element.ksize, 3);
        assert_eq!(element.vsize, 4);
    }

    // Test that converting a byte slice that is too small for a LeafElementHeader results in an error.
    // This ensures the system can handle corrupted leaf pages.
    #[test]
    fn test_leaf_page_element_try_from_too_small() {
        let data: [u8; LEAF_ELEMENT_HEADER_SIZE - 1] = [0; LEAF_ELEMENT_HEADER_SIZE - 1];
        let result = LeafElementHeader::try_from(&data as &[u8]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            Error::TooSmallData {
                expect: LEAF_ELEMENT_HEADER_SIZE,
                got: LEAF_ELEMENT_HEADER_SIZE - 1
            }
        );
    }

    // Test that a valid byte slice can be converted into a BucketHeader.
    // This is important for reading nested bucket information.
    #[test]
    fn test_bucket_try_from() {
        let mut data = [0; BUCKET_HEADER_SIZE];
        data[0..8].copy_from_slice(&1u64.to_le_bytes());
        data[8..16].copy_from_slice(&2u64.to_le_bytes());

        let bucket = BucketHeader::try_from(&data as &[u8]).unwrap();
        assert_eq!(bucket.root.0, 1);
        assert_eq!(bucket.sequence, 2);
    }

    // Test that converting a byte slice that is too small for a BucketHeader results in an error.
    // This protects against malformed bucket data.
    #[test]
    fn test_bucket_try_from_too_small() {
        let data: [u8; BUCKET_HEADER_SIZE - 1] = [0; BUCKET_HEADER_SIZE - 1];
        let result = BucketHeader::try_from(&data as &[u8]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            Error::TooSmallData {
                expect: BUCKET_HEADER_SIZE,
                got: BUCKET_HEADER_SIZE - 1
            }
        );
    }

    // Test the conversions and formatting for Pgid.
    // This ensures that page identifiers are handled correctly throughout the system.
    #[test]
    fn test_pgid() {
        let pgid: Pgid = 1u64.into();
        assert_eq!(pgid.0, 1);
        let id: u64 = pgid.into();
        assert_eq!(id, 1);
        assert_eq!(format!("{pgid}"), "1");
    }

    // Test the functionality of PageFlag.
    // This verifies that page types can be correctly identified and manipulated.
    #[test]
    fn test_page_flag() {
        let flag = PageFlag::BranchPageFlag;
        assert_eq!(flag.as_u16(), 1);
        assert!(flag.is_branch_page());
        assert!(!flag.is_leaf_page());
        assert!(!flag.is_meta_page());
        assert!(!flag.is_freelist_page());
        assert_eq!(format!("{flag:?}"), "PageFlag(BranchPageFlag)");
    }

    // Test the creation and basic properties of a Page.
    // This ensures that pages can be created from raw data and their headers can be accessed.
    #[test]
    fn test_page() {
        let mut data = vec![0; PAGE_HEADER_SIZE];
        data[0..8].copy_from_slice(&1u64.to_le_bytes());
        data[8..10].copy_from_slice(&PageFlag::BranchPageFlag.bits().to_le_bytes());

        let page = Page::new(data.clone(), PAGE_HEADER_SIZE).unwrap();
        assert_eq!(page.as_slice(), &data);
        let header = page.page_header();
        assert_eq!(header.id.0, 1);
        assert_eq!(header.flags, PageFlag::BranchPageFlag);
    }

    // Test the extraction of a KeyValue pair from a leaf page.
    // This is a fundamental operation for retrieving data from the database.
    #[test]
    fn test_key_value_from_page() {
        let mut data = vec![0; 100];
        // PageHeader
        data[8..10].copy_from_slice(&PageFlag::LeafPageFlag.bits().to_le_bytes());

        // LeafElementHeader
        let elem_start = PAGE_HEADER_SIZE;
        data[elem_start + 4..elem_start + 8].copy_from_slice(&16u32.to_le_bytes()); // pos
        data[elem_start + 8..elem_start + 12].copy_from_slice(&3u32.to_le_bytes()); // ksize
        data[elem_start + 12..elem_start + 16].copy_from_slice(&5u32.to_le_bytes()); // vsize

        // Data
        let data_start = elem_start + 16;
        data[data_start..data_start + 3].copy_from_slice(b"key");
        data[data_start + 3..data_start + 3 + 5].copy_from_slice(b"value");

        let elem_header = LeafElementHeader::try_from(&data[elem_start..]).unwrap();
        let kv = KeyValue::from_page(&data, &elem_header, 0).unwrap();
        assert_eq!(kv.key, b"key");
        assert_eq!(kv.value, b"value");
    }

    // Test the is_bucket method of LeafElementHeader.
    // This is important for distinguishing between key/value pairs and nested buckets.
    #[test]
    fn test_leaf_element_header_is_bucket() {
        let mut header = LeafElementHeader {
            flags: 1,
            pos: 0,
            ksize: 0,
            vsize: 0,
        };
        assert!(header.is_bucket());
        header.flags = 0;
        assert!(!header.is_bucket());
    }

    // Test the is_inline method of BucketHeader.
    // This is used to determine if a bucket's data is stored directly in the leaf page.
    #[test]
    fn test_bucket_header_is_inline() {
        let mut header = BucketHeader {
            root: Pgid(0),
            sequence: 0,
        };
        assert!(header.is_inline());
        header.root = Pgid(1);
        assert!(!header.is_inline());
    }

    // Test the parsing of a MetaPage and its contained Meta struct.
    // This ensures that the database's global metadata can be correctly read and verified.
    #[test]
    fn test_meta_page() {
        let mut data = vec![0; 128];
        // PageHeader
        data[8..10].copy_from_slice(&PageFlag::MetaPageFlag.bits().to_le_bytes());

        // Meta
        let meta_start = PAGE_HEADER_SIZE;
        data[meta_start..meta_start + 4].copy_from_slice(&MAGIC_NUMBER.to_le_bytes());
        data[meta_start + 4..meta_start + 8].copy_from_slice(&DATAFILE_VERSION.to_le_bytes());
        data[meta_start + 8..meta_start + 12].copy_from_slice(&4096u32.to_le_bytes());
        data[meta_start + 16..meta_start + 24].copy_from_slice(&3u64.to_le_bytes()); // root_pgid
        data[meta_start + 32..meta_start + 40].copy_from_slice(&4u64.to_le_bytes()); // freelist_pgid
        data[meta_start + 40..meta_start + 48].copy_from_slice(&10u64.to_le_bytes()); // max_pgid
        data[meta_start + 48..meta_start + 56].copy_from_slice(&100u64.to_le_bytes()); // txid

        let checksum =
            u64::from_be_bytes(Fnv64::hash(&data[16..72]).as_bytes().try_into().unwrap());
        data[72..80].copy_from_slice(&checksum.to_le_bytes());

        let page = MetaPage::new(data, 128).unwrap();
        let header = page.page_header();
        assert_eq!(header.flags, PageFlag::MetaPageFlag);

        let meta = page.meta().unwrap();
        assert_eq!(meta.magic, MAGIC_NUMBER);
        assert_eq!(meta.version, DATAFILE_VERSION);
        assert_eq!(meta.page_size, 4096);
        assert_eq!(meta.root_pgid.0, 3);
        assert_eq!(meta.freelist_pgid.0, 4);
        assert_eq!(meta.max_pgid.0, 10);
        assert_eq!(meta.txid, 100);
        assert_eq!(meta.checksum, checksum);
    }

    // Test the parsing of a FreelistPage to extract the list of free page IDs.
    // This is essential for the database to be able to reuse deallocated pages.
    #[test]
    fn test_freelist_page() {
        let mut data = vec![0; 128];
        // PageHeader
        data[8..10].copy_from_slice(&PageFlag::FreelistPageFlag.bits().to_le_bytes());
        data[10..12].copy_from_slice(&3u16.to_le_bytes()); // count

        // Page IDs
        let pids_start = PAGE_HEADER_SIZE;
        data[pids_start..pids_start + 8].copy_from_slice(&10u64.to_le_bytes());
        data[pids_start + 8..pids_start + 16].copy_from_slice(&11u64.to_le_bytes());
        data[pids_start + 16..pids_start + 24].copy_from_slice(&12u64.to_le_bytes());

        let page = FreelistPage::new(data, 128).unwrap();
        let header = page.page_header();
        assert_eq!(header.flags, PageFlag::FreelistPageFlag);

        let free_pages = page.free_pages().unwrap();
        assert_eq!(free_pages, vec![Pgid(10), Pgid(11), Pgid(12)]);
    }

    // Test the parsing of a BranchPage to extract its elements.
    // This is a key part of traversing the B-tree structure.
    #[test]
    fn test_branch_page() {
        let mut data = vec![0; 128];
        // PageHeader
        data[8..10].copy_from_slice(&PageFlag::BranchPageFlag.bits().to_le_bytes());
        data[10..12].copy_from_slice(&1u16.to_le_bytes()); // count

        // BranchElementHeader
        let elem_start = PAGE_HEADER_SIZE;
        data[elem_start..elem_start + 4].copy_from_slice(&16u32.to_le_bytes()); // pos
        data[elem_start + 4..elem_start + 8].copy_from_slice(&3u32.to_le_bytes()); // ksize
        data[elem_start + 8..elem_start + 16].copy_from_slice(&5u64.to_le_bytes()); // pgid

        // Data
        let data_start = elem_start + 16;
        data[data_start..data_start + 3].copy_from_slice(b"key");

        let page = BranchPage::new(data, 128).unwrap();
        let header = page.page_header();
        assert_eq!(header.flags, PageFlag::BranchPageFlag);

        let elements = page.branch_elements().unwrap();
        assert_eq!(elements.len(), 1);
        assert_eq!(elements[0].key, b"key");
        assert_eq!(elements[0].pgid.0, 5);
    }

    // Test the parsing of a LeafPage to extract its elements.
    // This is fundamental for reading the actual data stored in the database.
    #[test]
    fn test_leaf_page() {
        let mut data = vec![0; 128];
        // PageHeader
        data[8..10].copy_from_slice(&PageFlag::LeafPageFlag.bits().to_le_bytes());
        data[10..12].copy_from_slice(&1u16.to_le_bytes()); // count

        // LeafElementHeader
        let elem_start = PAGE_HEADER_SIZE;
        data[elem_start + 4..elem_start + 8].copy_from_slice(&16u32.to_le_bytes()); // pos
        data[elem_start + 8..elem_start + 12].copy_from_slice(&3u32.to_le_bytes()); // ksize
        data[elem_start + 12..elem_start + 16].copy_from_slice(&5u32.to_le_bytes()); // vsize

        // Data
        let data_start = elem_start + 16;
        data[data_start..data_start + 3].copy_from_slice(b"key");
        data[data_start + 3..data_start + 3 + 5].copy_from_slice(b"value");

        let page = LeafPage::new(data, 128).unwrap();
        let header = page.page_header();
        assert_eq!(header.flags, PageFlag::LeafPageFlag);

        let elements = page.leaf_elements().unwrap();
        assert_eq!(elements.len(), 1);
        match &elements[0] {
            LeafElement::KeyValue(kv) => {
                assert_eq!(kv.key, b"key");
                assert_eq!(kv.value, b"value");
            }
            _ => panic!("unexpected element type"),
        }
    }

    // Test the from_page method for BranchElement.
    // This ensures that individual branch elements can be correctly parsed from a page.
    #[test]
    fn test_branch_element_from_page() {
        let mut data = vec![0; 100];
        // PageHeader
        data[8..10].copy_from_slice(&PageFlag::BranchPageFlag.bits().to_le_bytes());

        // BranchElementHeader
        let elem_start = PAGE_HEADER_SIZE;
        data[elem_start..elem_start + 4].copy_from_slice(&16u32.to_le_bytes()); // pos
        data[elem_start + 4..elem_start + 8].copy_from_slice(&3u32.to_le_bytes()); // ksize
        data[elem_start + 8..elem_start + 16].copy_from_slice(&5u64.to_le_bytes()); // pgid

        // Data
        let data_start = elem_start + 16;
        data[data_start..data_start + 3].copy_from_slice(b"key");

        let page = BranchPage::new(data, 100).unwrap();
        let elem_header = BranchElementHeader::try_from(&page.data[elem_start..]).unwrap();
        let element = BranchElement::from_page(&page, &elem_header, 0).unwrap();
        assert_eq!(element.key, b"key");
        assert_eq!(element.pgid.0, 5);
    }

    // Test the from_page method for a KeyValue LeafElement.
    // This verifies that key-value pairs are correctly extracted from leaf pages.
    #[test]
    fn test_leaf_element_from_page_kv() {
        let mut data = vec![0; 100];
        // PageHeader
        data[8..10].copy_from_slice(&PageFlag::LeafPageFlag.bits().to_le_bytes());

        // LeafElementHeader
        let elem_start = PAGE_HEADER_SIZE;
        data[elem_start + 4..elem_start + 8].copy_from_slice(&16u32.to_le_bytes()); // pos
        data[elem_start + 8..elem_start + 12].copy_from_slice(&3u32.to_le_bytes()); // ksize
        data[elem_start + 12..elem_start + 16].copy_from_slice(&5u32.to_le_bytes()); // vsize

        // Data
        let data_start = elem_start + 16;
        data[data_start..data_start + 3].copy_from_slice(b"key");
        data[data_start + 3..data_start + 3 + 5].copy_from_slice(b"value");

        let page = LeafPage::new(data, 100).unwrap();
        let elem_header = LeafElementHeader::try_from(&page.data[elem_start..]).unwrap();
        let element = LeafElement::from_page(&page, &elem_header, 0).unwrap();
        match element {
            LeafElement::KeyValue(kv) => {
                assert_eq!(kv.key, b"key");
                assert_eq!(kv.value, b"value");
            }
            _ => panic!("unexpected element type"),
        }
    }

    // Test the from_page method for a Bucket LeafElement.
    // This ensures that nested buckets are correctly identified and parsed.
    #[test]
    fn test_leaf_element_from_page_bucket() {
        let mut data = vec![0; 128];
        // PageHeader
        data[8..10].copy_from_slice(&PageFlag::LeafPageFlag.bits().to_le_bytes());
        data[10..12].copy_from_slice(&1u16.to_le_bytes()); // count

        // LeafElementHeader
        let elem_start = PAGE_HEADER_SIZE;
        data[elem_start..elem_start + 4].copy_from_slice(&1u32.to_le_bytes()); // flags (bucket)
        data[elem_start + 4..elem_start + 8].copy_from_slice(&16u32.to_le_bytes()); // pos
        data[elem_start + 8..elem_start + 12].copy_from_slice(&4u32.to_le_bytes()); // ksize
        data[elem_start + 12..elem_start + 16]
            .copy_from_slice(&(BUCKET_HEADER_SIZE as u32).to_le_bytes()); // vsize (BucketHeader size)

        // Data
        let data_start = elem_start + 16;
        data[data_start..data_start + 4].copy_from_slice(b"name");
        // BucketHeader
        data[data_start + 4..data_start + 4 + 8].copy_from_slice(&7u64.to_le_bytes()); // root pgid

        let page = LeafPage::new(data, 128).unwrap();
        let elem_header = LeafElementHeader::try_from(&page.data[elem_start..]).unwrap();
        let element = LeafElement::from_page(&page, &elem_header, 0).unwrap();
        match element {
            LeafElement::Bucket {
                name,
                root_pgid,
                pgid,
            } => {
                assert_eq!(name, b"name");
                assert_eq!(root_pgid.0, 7);
                assert_eq!(pgid.0, 0);
            }
            _ => panic!("unexpected element type"),
        }
    }

    #[test]
    fn test_page_capacity() {
        let page_size = 4096;
        let mut data = vec![0; page_size * 2];
        // PageHeader with overflow = 1
        data[12..16].copy_from_slice(&1u32.to_le_bytes());
        data[8..10].copy_from_slice(&PageFlag::LeafPageFlag.bits().to_le_bytes());
        let page = Page::new(data, page_size).unwrap();
        assert_eq!(page.capacity(), (1 + 1) * page_size);

        let mut data = vec![0; page_size];
        // PageHeader with overflow = 0
        data[12..16].copy_from_slice(&0u32.to_le_bytes());
        data[8..10].copy_from_slice(&PageFlag::LeafPageFlag.bits().to_le_bytes());
        let page = Page::new(data, page_size).unwrap();
        assert_eq!(page.capacity(), page_size);
    }

    #[test]
    fn test_page_alignment_check() {
        let page_size = 4096;
        let mut data = vec![0; page_size + 1]; // Not aligned
        data[8..10].copy_from_slice(&PageFlag::LeafPageFlag.bits().to_le_bytes());

        let result = Page::new(data, page_size);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            Error::InvalidData("data size mismatch with page size and overflow")
        );
    }
}
