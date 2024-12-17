use std::io::{Cursor, Read, Seek, SeekFrom};

use esedb_macros::ReadFromAndWriteToBytes;
use from_to_repr::from_to_other;

use crate::bitflags_read_write_bytes;
use crate::byte_io::{ByteRead, LittleEndianRead, ReadFromBytes, WriteToBytes};
use crate::common::DbTime;
use crate::error::{ReadError, WriteError};
use crate::header::Header;


/// The maximum size of a small page. If the page size is greater than this, page tags switch from
/// the small to the large format.
const MAX_SIZE_SMALL_PAGE: u32 = 1024 * 8;


#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageHeader {
    pub checksum_and_page_number: ChecksumAndPageNumber,
    pub last_modification_time: DbTime,
    pub prev_page_num: u32,
    pub next_page_num: u32,
    pub father_data_page_oid: u32,
    pub available_data_size: u16,
    pub available_uncommitted_data_size: u16,
    pub first_available_data_offset: u16,
    pub first_available_page_tag: u16,
    pub flags: PageFlags,
}
impl PageHeader {
    pub fn page_number(&self) -> u64 { self.checksum_and_page_number.page_number() }

    pub fn size_bytes(&self) -> u64 {
        match &self.checksum_and_page_number {
            ChecksumAndPageNumber::V1 { .. } => 40,
            ChecksumAndPageNumber::V2 { .. } => 80,
            ChecksumAndPageNumber::V3 { .. } => 80,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ChecksumAndPageNumber {
    V1 {
        xor_checksum: u32,
        page_number: u32,
    },
    V2 {
        xor_checksum: u32,
        ecc_checksum: u32,
        extended_header: ExtendedPageHeader,
    },
    V3 {
        checksum: u64,
        extended_header: ExtendedPageHeader,
    },
}
impl ChecksumAndPageNumber {
    pub fn page_number(&self) -> u64 {
        match self {
            Self::V1 { page_number, .. } => (*page_number).into(),
            Self::V2 { extended_header, .. } => extended_header.page_number,
            Self::V3 { extended_header, .. } => extended_header.page_number,
        }
    }
}


#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
struct RawPageHeader {
    pub checksum_and_page_number_value: u64,
    pub last_modification_time: DbTime, // u64
    pub prev_page_num: u32,
    pub next_page_num: u32,
    pub father_data_page_oid: u32,
    pub available_data_size: u16,
    pub available_uncommitted_data_size: u16,
    pub first_available_data_offset: u16,
    pub first_available_page_tag: u16,
    pub flags: PageFlags, // u32
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
pub struct ExtendedPageHeader {
    pub extended_checksum_1: u64,
    pub extended_checksum_2: u64,
    pub extended_checksum_3: u64,
    pub page_number: u64,
    pub unknown: u64,
}


bitflags_read_write_bytes! {
    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    pub struct PageFlags : u32 {
        const ROOT_PAGE = 0x0000_0001;
        const LEAF_PAGE = 0x0000_0002;
        const BRANCH_PAGE = 0x0000_0004;
        const EMPTY_PAGE = 0x0000_0008;
        const REPAIR_PAGE = 0x0000_0010;
        const SPACE_TREE_PAGE = 0x0000_0020;
        const INDEX_PAGE = 0x0000_0040;
        const LONG_VALUE_PAGE = 0x0000_0080;
        const SLV_AVAIL_PAGE = 0x0000_0100;
        const SLV_OWNER_MAP_PAGE = 0x0000_0200;
        const NON_UNIQUE_KEYS = 0x0000_0400;
        const NEW_RECORD_FORMAT = 0x0000_0800;
        // 0x0000_1000 missing
        const NEW_CHECKSUM_FORMAT = 0x0000_2000;
        const SCRUBBED = 0x0000_4000;

        // bits 0x0001_8000 are a bitmask for flush behavior
        const ROCK_WRITE = 0x0000_8000;
        const PAPER_WRITE = 0x0001_0000;
        const SCISSORS_WRITE = 0x0001_8000;

        const PREINITIALIZED = 0x0002_0000;
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct PageTagSmall {
    pub value_offset: u16, // u13
    pub flags: PageTagFlags, // u3
    pub value_size: u16, // u13
    pub unknown: u8, // u3
}
impl ReadFromBytes for PageTagSmall {
    type Error = std::io::Error;
    fn read_from_bytes<R: crate::byte_io::ByteRead>(reader: &mut R) -> Result<Self, Self::Error> {
        let value_size_and_flags = reader.read_u16()?;
        let value_offset_and_flags = reader.read_u16()?;
        // the flags are in the topmost bits

        let value_offset = (value_offset_and_flags >> 0) & 0b0001_1111_1111_1111;
        let flags_u8 = u8::try_from((value_offset_and_flags >> 13) & 0b0111).unwrap();
        let flags = PageTagFlags::from_bits_retain(flags_u8);
        let value_size = (value_size_and_flags >> 0) & 0b0001_1111_1111_1111;
        let unknown = u8::try_from((value_size_and_flags >> 13) & 0b0111).unwrap();
        Ok(Self {
            value_offset,
            flags,
            value_size,
            unknown,
        })
    }
}
impl WriteToBytes for PageTagSmall {
    type Error = WriteError;
    fn write_to_bytes<W: crate::byte_io::ByteWrite>(&self, writer: &mut W) -> Result<(), Self::Error> {
        WriteError::check_unsigned_max("value_offset", self.value_offset.into(), 0b0001_1111_1111_1111)?;
        let flags_u8 = self.flags.bits();
        WriteError::check_unsigned_max("flags", flags_u8.into(), 0b111)?;
        WriteError::check_unsigned_max("value_size", self.value_size.into(), 0b0001_1111_1111_1111)?;
        WriteError::check_unsigned_max("unknown", self.unknown.into(), 0b111)?;

        let value_size_and_flags =
            (self.value_size << 0)
            | (u16::from(self.unknown) << 13)
        ;
        let value_offset_and_flags =
            (self.value_offset << 0)
            | (u16::from(flags_u8) << 13)
        ;

        writer.write_u16(value_size_and_flags)?;
        writer.write_u16(value_offset_and_flags)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct PageTagLarge {
    pub value_offset: u16, // u15
    pub offset_flag: bool, // u1
    pub value_size: u16, // u15
    pub size_flag: bool, // u1
}
impl ReadFromBytes for PageTagLarge {
    type Error = std::io::Error;
    fn read_from_bytes<R: crate::byte_io::ByteRead>(reader: &mut R) -> Result<Self, Self::Error> {
        let value_size_and_flags = reader.read_u16()?;
        let value_offset_and_flags = reader.read_u16()?;
        // the flags are in the topmost bits

        let value_offset = (value_offset_and_flags >> 0) & 0b0111_1111_1111_1111;
        let offset_flag = (value_offset_and_flags & (1 << 15)) != 0;
        let value_size = (value_size_and_flags >> 0) & 0b0111_1111_1111_1111;
        let size_flag = (value_size_and_flags & (1 << 15)) != 0;
        Ok(Self {
            value_offset,
            offset_flag,
            value_size,
            size_flag,
        })
    }
}
impl WriteToBytes for PageTagLarge {
    type Error = WriteError;
    fn write_to_bytes<W: crate::byte_io::ByteWrite>(&self, writer: &mut W) -> Result<(), Self::Error> {
        WriteError::check_unsigned_max("value_offset", self.value_offset.into(), 0b0111_1111_1111_1111)?;
        WriteError::check_unsigned_max("value_size", self.value_size.into(), 0b0111_1111_1111_1111)?;

        let full_tag: u32 =
            (u32::from(self.value_offset) << 17)
            | if self.offset_flag { 1 << 16 } else { 0 }
            | (u32::from(self.value_size) << 1)
            | if self.size_flag { 1 << 0 } else { 0 }
        ;
        writer.write_u32(full_tag)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageTag {
    pub value_offset: u16,
    pub value_size: u16,
    pub flags: PageTagFlags,
}

bitflags_read_write_bytes! {
    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    pub struct PageTagFlags : u8 {
        const VERSION = 0x1;
        const DELETED = 0x2;
        const COMPRESSED = 0x4;
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
pub struct RootPageHeaderShort {
    pub initial_page_count: u32,
    pub parent_father_data_page_number: u32,
    pub extent_space: ExtentSpace, // u32
    pub space_tree_page_number: u32,
}

#[derive(Clone, Copy, Debug, ReadFromAndWriteToBytes)]
#[from_to_other(base_type = u32, derive_compare = "as_int")]
pub enum ExtentSpace {
    Single = 0,
    Multiple = 1,
    Other(u32),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
pub struct RootPageHeaderLong {
    pub initial_page_count: u32,
    pub unknown1: u8,
    pub parent_father_data_page_number: u32,
    pub extent_space: ExtentSpace, // u32
    pub space_tree_page_number: u32,
    pub unknown2: u32,
    pub unknown3: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RootPageHeader {
    Short(RootPageHeaderShort),
    Long(RootPageHeaderLong),
}
macro_rules! impl_rph_variant {
    ($name:ident, $type:ty) => {
        pub fn $name(&self) -> $type {
            match self {
                Self::Short(h) => h.$name,
                Self::Long(h) => h.$name,
            }
        }
    };
}
impl RootPageHeader {
    impl_rph_variant!(initial_page_count, u32);
    impl_rph_variant!(parent_father_data_page_number, u32);
    impl_rph_variant!(extent_space, ExtentSpace);
    impl_rph_variant!(space_tree_page_number, u32);
}

pub fn page_byte_offset(page_size: u32, page_number: u64) -> Result<u64, ReadError> {
    // pages are numbered starting at 1
    // however, the first two pages are header and shadow header
    // => page number 1 has page index 2; page number p has page index (p + 1)

    if page_number == 0 {
        return Err(ReadError::Page0);
    }

    let page_index = page_number + 1;
    let byte_offset = page_index * u64::from(page_size);
    Ok(byte_offset)
}

pub fn read_page_header<R: Read + Seek>(reader: &mut R, header: &Header, page_number: u64) -> Result<PageHeader, ReadError> {
    let byte_offset = page_byte_offset(header.page_size, page_number)?;
    reader.seek(SeekFrom::Start(byte_offset))?;

    // read raw page header
    let mut read = LittleEndianRead::new(reader);
    let raw_header = RawPageHeader::read_from_bytes(&mut read)?;
    let checksum_and_page_number = if raw_header.flags.contains(PageFlags::NEW_CHECKSUM_FORMAT) {
        // longer header
        let extended_header = ExtendedPageHeader::read_from_bytes(&mut read)?;
        if header.version_and_revision() >= 0x0000_0620_0000_0011 {
            ChecksumAndPageNumber::V3 { checksum: raw_header.checksum_and_page_number_value, extended_header }
        } else {
            let xor_checksum: u32 = ((raw_header.checksum_and_page_number_value >>  0) & 0xFFFF_FFFF).try_into().unwrap();
            let ecc_checksum: u32 = ((raw_header.checksum_and_page_number_value >> 32) & 0xFFFF_FFFF).try_into().unwrap();
            ChecksumAndPageNumber::V2 { xor_checksum, ecc_checksum, extended_header }
        }
    } else {
        let xor_checksum: u32 = ((raw_header.checksum_and_page_number_value >>  0) & 0xFFFF_FFFF).try_into().unwrap();
        let page_number: u32 = ((raw_header.checksum_and_page_number_value >> 32) & 0xFFFF_FFFF).try_into().unwrap();
        ChecksumAndPageNumber::V1 { xor_checksum, page_number }
    };

    Ok(PageHeader {
        checksum_and_page_number,
        last_modification_time: raw_header.last_modification_time,
        prev_page_num: raw_header.prev_page_num,
        next_page_num: raw_header.next_page_num,
        father_data_page_oid: raw_header.father_data_page_oid,
        available_data_size: raw_header.available_data_size,
        available_uncommitted_data_size: raw_header.available_uncommitted_data_size,
        first_available_data_offset: raw_header.first_available_data_offset,
        first_available_page_tag: raw_header.first_available_page_tag,
        flags: raw_header.flags,
    })
}

pub fn read_page_tags<R: Read + Seek>(reader: &mut R, header: &Header, page_header: &PageHeader) -> Result<Vec<PageTag>, ReadError> {
    // tags are at the end of the page => skip to the beginning of the next page minus the tags used
    let tag_count = u64::from(page_header.first_available_page_tag);
    let tag_byte_count = 4 * tag_count;
    let next_page_byte_offset = page_byte_offset(header.page_size, page_header.page_number() + 1)?;
    let byte_offset = next_page_byte_offset - tag_byte_count;
    reader.seek(SeekFrom::Start(byte_offset))?;

    let mut read = LittleEndianRead::new(reader);

    let tag_count_usize: usize = tag_count.try_into().unwrap();
    let mut tags = Vec::with_capacity(tag_count_usize);
    if header.page_size <= MAX_SIZE_SMALL_PAGE {
        // small tags
        for _ in 0..tag_count_usize {
            let tag = PageTagSmall::read_from_bytes(&mut read)?;
            tags.push(PageTag {
                value_offset: tag.value_offset,
                value_size: tag.value_size,
                flags: tag.flags,
            });
        }
    } else {
        // large tags
        // flags are stored in the upper bits of the u16 at the beginning of the data
        // (you wanted to store your own data in there? haha nope)
        // if there's less than two bytes of data, the flags are 0
        let mut tags = Vec::with_capacity(tag_count_usize);
        for _ in 0..tag_count_usize {
            let tag = PageTagLarge::read_from_bytes(&mut read)?;
            let flags = if tag.value_size >= 2 {
                let orig_pos = read.stream_position()?;

                let page_offset = page_byte_offset(header.page_size, page_header.page_number())?;
                let page_header_length = page_header.size_bytes();
                let tag_data_offset: u64 = tag.value_offset.into();
                let tag_data_pos = page_offset + page_header_length + tag_data_offset;
                read.seek(SeekFrom::Start(tag_data_pos))?;
                let flags_u16 = read.read_u16()?;
                // only the top 3 bits count
                let flags_u3 = u8::try_from((flags_u16 >> 13) & 0b111).unwrap();
                let flags = PageTagFlags::from_bits_retain(flags_u3);

                read.seek(SeekFrom::Start(orig_pos))?;
                flags
            } else {
                PageTagFlags::empty()
            };
            tags.push(PageTag {
                value_offset: tag.value_offset,
                value_size: tag.value_size,
                flags,
            });
        }
    }
    tags.reverse();
    Ok(tags)
}

pub fn read_data_for_tag<R: Read + Seek>(reader: &mut R, header: &Header, page_header: &PageHeader, tag: &PageTag) -> Result<Vec<u8>, ReadError> {
    let page_offset = page_byte_offset(header.page_size, page_header.page_number())?;
    let page_header_size = page_header.size_bytes();
    let tag_offset: u64 = tag.value_offset.into();
    let tag_length: usize = tag.value_size.into();

    reader.seek(SeekFrom::Start(page_offset + page_header_size + tag_offset))?;

    let mut buf = vec![0u8; tag_length];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn read_root_page_header(data: &[u8]) -> Result<RootPageHeader, ReadError> {
    let cursor = Cursor::new(data);
    let mut read = LittleEndianRead::new(cursor);

    if data.len() == 16 {
        let short = RootPageHeaderShort::read_from_bytes(&mut read)?;
        Ok(RootPageHeader::Short(short))
    } else if data.len() == 25 {
        let long = RootPageHeaderLong::read_from_bytes(&mut read)?;
        Ok(RootPageHeader::Long(long))
    } else {
        Err(ReadError::UnknownFormatVariant)
    }
}