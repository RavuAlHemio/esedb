use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::{Cursor, Read, Seek};
use std::sync::LazyLock;

use bitflags::bitflags;
use encoding_rs::DecoderResult;
use from_to_repr::from_to_other;
use tracing::{instrument, trace};
use uuid::Uuid;

use crate::byte_io::{ByteRead, LittleEndianRead};
use crate::data::{Bit, Data, DataType};
use crate::error::ReadError;
use crate::header::Header;
use crate::page::{MAX_SIZE_SMALL_PAGE, read_data_from_tree};


// here we have a bit of a bootstrapping issue
// the structure of the metadata tables is stored in the database
// but we must know the structure of the metadata tables to read the metadata tables
// => define a minimal schema for the initial read


bitflags! {
    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    pub struct ObjectFlags : i32 {
        const SYSTEM = 0x8000_0000u32 as i32;
        const TABLE_FIXED_DDL = 0x4000_0000;
        const TABLE_TEMPLATE = 0x2000_0000;
        const TABLE_DERIVED = 0x1000_0000;
        const DYNAMIC = 0x0800_0000; // generally used in conjunction with SYSTEM
        const NO_FIXED_VAR_COLUMNS_IN_DERIVED_TABLES = 0x0400_0000;
    }
}

bitflags! {
    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    pub struct ColumnFlags : i32 {
        const FIXED = 0x0000_0001;
        const TAGGED = 0x0000_0002;
        const NOT_NULL = 0x0000_0004;
        const VERSION = 0x0000_0008;
        const AUTOINCREMENT = 0x0000_0010;
        const UPDATABLE = 0x0000_0020;
        const TEMP_TABLE_KEY = 0x0000_0040;
        const TEMP_TABLE_DESCENDING = 0x0000_0080;
        // no 0x000_0100
        // no 0x000_0200
        const MULTI_VALUED = 0x0000_0400;
        const ESCROW_UPDATE = 0x0000_0800;
        const UNVERSIONED = 0x0000_1000;
        const MAYBE_NULL = 0x0000_2000;
        const FINALIZE = 0x0000_4000;
        const USER_DEFINED_DEFAULT = 0x0000_8000;
        const RENAME_CONVERT_TO_PRIMARY_INDEX_PLACEHOLDER = 0x0001_0000;
        const DELETE_ON_ZERO = 0x0002_0000;
        const VARIABLE = 0x0004_0000;
        const COMPRESSED = 0x0008_0000;
        const ENCRYPTED = 0x0010_0000;
    }
}


macro_rules! get_value {
    (@required, $name_to_column:expr, $values:expr, $name:expr, $expected_type:tt) => {
        {
            let Some(column_def) = $name_to_column.get($name) else {
                return Err(ReadError::MissingRequiredColumn { name: Cow::Borrowed($name) })
            };
            let Some(value) = $values.get(&column_def.column_id) else {
                return Err(ReadError::MissingRequiredColumn { name: Cow::Borrowed($name) })
            };
            let Value::Simple(data) = value else {
                // let's pretend it's missing
                return Err(ReadError::MissingRequiredColumn { name: Cow::Borrowed($name) })
            };
            if let Data::$expected_type(inner) = data {
                inner
            } else {
                return Err(ReadError::WrongColumnType { name: Cow::Borrowed($name), expected: DataType::$expected_type, obtained: data.data_type() });
            }
        }
    };
    (@optional_deref, $name_to_column:expr, $values:expr, $name:expr, $expected_type:tt) => {
        {
            if let Some(column_def) = $name_to_column.get($name) {
                if let Some(value) = $values.get(&column_def.column_id) {
                    if let Value::Simple(data) = value {
                        if let Data::Nil = data {
                            None
                        } else if let Data::$expected_type(inner) = data {
                            Some(*inner)
                        } else {
                            return Err(ReadError::WrongColumnType { name: Cow::Borrowed($name), expected: DataType::$expected_type, obtained: data.data_type() });
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
    };
}


#[derive(Clone, Copy, Debug)]
#[from_to_other(base_type = i16, derive_compare = "as_int")]
pub enum ObjectType {
    Table = 0x0001,
    Column = 0x0002,
    Index = 0x0003,
    LongValue = 0x0004,
    Callback = 0x0005,
    Other(i16),
}


#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TableHeader {
    pub table_object_id: i32,
    pub fdp_object_id: i32,
    pub fdp_page_number: i32,
    pub density: i32,
    pub flags: ObjectFlags,
    pub page_count: i32,
    pub root_flag: Option<Bit>,
    pub name: String,
}
impl TableHeader {
    #[instrument]
    pub fn try_from_metadata(column_defs: &[Column], values: &BTreeMap<i32, Value>) -> Result<Self, ReadError> {
        let name_to_column = get_name_to_column(column_defs);

        let type_value_i16 = *get_value!(@required, name_to_column, values, "Type", Short);
        let type_value = ObjectType::from_base_type(type_value_i16);
        ReadError::ensure_object_type(ObjectType::Table, type_value)?;

        let table_object_id = *get_value!(@required, name_to_column, values, "ObjidTable", Long);
        let fdp_object_id = *get_value!(@required, name_to_column, values, "Id", Long);
        let fdp_page_number = *get_value!(@required, name_to_column, values, "ColtypOrPgnoFDP", Long);
        let density = *get_value!(@required, name_to_column, values, "SpaceUsage", Long);
        let flags_i32 = *get_value!(@required, name_to_column, values, "Flags", Long);
        let flags = ObjectFlags::from_bits_retain(flags_i32);
        let page_count = *get_value!(@required, name_to_column, values, "PagesOrLocale", Long);
        let root_flag = get_value!(@optional_deref, name_to_column, values, "RootFlag", Bit);
        let name = get_value!(@required, name_to_column, values, "Name", Text);

        Ok(TableHeader {
            table_object_id,
            fdp_object_id,
            fdp_page_number,
            density,
            flags,
            page_count,
            root_flag,
            name: name.clone(),
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Column {
    pub table_object_id: i32,
    pub column_id: i32,
    pub column_type: DataType,
    pub length: i32,
    pub flags: ColumnFlags,
    pub codepage: i32,
    pub root_flag: Option<Bit>,
    pub record_offset: Option<i16>,
    pub name: String,
}
impl Column {
    #[instrument]
    pub fn try_from_metadata(column_defs: &[Column], values: &BTreeMap<i32, Value>) -> Result<Self, ReadError> {
        let name_to_column = get_name_to_column(column_defs);

        let type_value_i16 = *get_value!(@required, name_to_column, values, "Type", Short);
        let type_value = ObjectType::from_base_type(type_value_i16);
        ReadError::ensure_object_type(ObjectType::Column, type_value)?;

        let table_object_id = *get_value!(@required, name_to_column, values, "ObjidTable", Long);
        let column_id = *get_value!(@required, name_to_column, values, "Id", Long);
        let column_type_i32 = *get_value!(@required, name_to_column, values, "ColtypOrPgnoFDP", Long);
        let column_type = DataType::from_base_type(column_type_i32);
        let length = *get_value!(@required, name_to_column, values, "SpaceUsage", Long);
        let flags_i32 = *get_value!(@required, name_to_column, values, "Flags", Long);
        let flags = ColumnFlags::from_bits_retain(flags_i32);
        let codepage = *get_value!(@required, name_to_column, values, "PagesOrLocale", Long);
        let root_flag = get_value!(@optional_deref, name_to_column, values, "RootFlag", Bit);
        let record_offset = get_value!(@optional_deref, name_to_column, values, "RecordOffset", Short);
        let name = get_value!(@required, name_to_column, values, "Name", Text);

        Ok(Column {
            table_object_id,
            column_id,
            column_type,
            length,
            flags,
            codepage,
            root_flag,
            record_offset,
            name: name.clone(),
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Index {
    pub table_object_id: i32,
    pub index_id: i32,
    pub fdp_page_number: i32,
    pub used_pages: i32,
    pub flags: ObjectFlags,
    pub locale: i32,
    pub root_flag: Option<Bit>,
    pub record_offset: Option<i16>,
    pub name: String,
}
impl Index {
    #[instrument]
    pub fn try_from_metadata(column_defs: &[Column], values: &BTreeMap<i32, Value>) -> Result<Self, ReadError> {
        let name_to_column = get_name_to_column(column_defs);

        let type_value_i16 = *get_value!(@required, name_to_column, values, "Type", Short);
        let type_value = ObjectType::from_base_type(type_value_i16);
        ReadError::ensure_object_type(ObjectType::Index, type_value)?;

        let table_object_id = *get_value!(@required, name_to_column, values, "ObjidTable", Long);
        let index_id = *get_value!(@required, name_to_column, values, "Id", Long);
        let fdp_page_number = *get_value!(@required, name_to_column, values, "ColtypOrPgnoFDP", Long);
        let used_pages = *get_value!(@required, name_to_column, values, "SpaceUsage", Long);
        let flags_i32 = *get_value!(@required, name_to_column, values, "Flags", Long);
        let flags = ObjectFlags::from_bits_retain(flags_i32);
        let locale = *get_value!(@required, name_to_column, values, "PagesOrLocale", Long);
        let root_flag = get_value!(@optional_deref, name_to_column, values, "RootFlag", Bit);
        let record_offset = get_value!(@optional_deref, name_to_column, values, "RecordOffset", Short);
        let name = get_value!(@required, name_to_column, values, "Name", Text);

        Ok(Index {
            table_object_id,
            index_id,
            fdp_page_number,
            used_pages,
            flags,
            locale,
            root_flag,
            record_offset,
            name: name.clone(),
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LongValueInfo {
    pub table_object_id: i32,
    pub long_value_id: i32,
    pub fdp_page_number: i32,
    pub used_pages: i32,
    pub flags: ObjectFlags,
    pub page_count: i32,
    pub name: String,
}
impl LongValueInfo {
    #[instrument]
    pub fn try_from_metadata(column_defs: &[Column], values: &BTreeMap<i32, Value>) -> Result<Self, ReadError> {
        let name_to_column = get_name_to_column(column_defs);

        let type_value_i16 = *get_value!(@required, name_to_column, values, "Type", Short);
        let type_value = ObjectType::from_base_type(type_value_i16);
        ReadError::ensure_object_type(ObjectType::LongValue, type_value)?;

        let table_object_id = *get_value!(@required, name_to_column, values, "ObjidTable", Long);
        let long_value_id = *get_value!(@required, name_to_column, values, "Id", Long);
        let fdp_page_number = *get_value!(@required, name_to_column, values, "ColtypOrPgnoFDP", Long);
        let used_pages = *get_value!(@required, name_to_column, values, "SpaceUsage", Long);
        let flags_i32 = *get_value!(@required, name_to_column, values, "Flags", Long);
        let flags = ObjectFlags::from_bits_retain(flags_i32);
        let page_count = *get_value!(@required, name_to_column, values, "PagesOrLocale", Long);
        let name = get_value!(@required, name_to_column, values, "Name", Text);

        Ok(LongValueInfo {
            table_object_id,
            long_value_id,
            fdp_page_number,
            used_pages,
            flags,
            page_count,
            name: name.clone(),
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Table {
    pub header: TableHeader,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>,
    pub long_value: Option<LongValueInfo>,
}
impl Table {
    pub fn long_value_page_number(&self) -> Option<u64> {
        self.long_value.as_ref()
            .map(|lv| lv.fdp_page_number.try_into().unwrap())
    }
}

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    pub struct SmallTagFlags : u16 {
        const NULL = 0x2000;
        const HAS_EXTENDED_FLAGS = 0x4000;
        const DERIVED = 0x8000;
    }

    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    pub struct TagFlags : u8 {
        const LONG_VALUE = 0x01;
        const COMPRESSED = 0x02;
        const SEPARATED = 0x04;
        const MULTI_VALUES = 0x08;
        const TWO_VALUES = 0x10;
        const NULL = 0x20;
        const ENCRYPTED = 0x40;
        const DERIVED = 0x80;
    }
}
impl TagFlags {
    pub fn from_small(small_flags: SmallTagFlags) -> Self {
        let mut ret = Self::empty();
        if small_flags.contains(SmallTagFlags::NULL) {
            ret |= TagFlags::NULL;
        }
        // we cannot transfer HAS_EXTENDED_FLAGS
        if small_flags.contains(SmallTagFlags::DERIVED) {
            ret |= TagFlags::DERIVED;
        }
        ret
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Value {
    Simple(Data),
    Complex {
        data: Data,
        flags: TagFlags,
    },
    Multiple {
        values: Vec<Data>,
        flags: TagFlags,
    },
}


pub static METADATA_COLUMN_DEFS: LazyLock<[Column; 10]> = LazyLock::new(|| [
    Column {
        table_object_id: 2,
        column_id: 1,
        column_type: DataType::Long,
        length: 4,
        flags: ColumnFlags::FIXED,
        codepage: 1252,
        root_flag: None,
        record_offset: Some(4),
        name: "ObjidTable".to_owned(),
    },
    Column {
        table_object_id: 2,
        column_id: 2,
        column_type: DataType::Short,
        length: 2,
        flags: ColumnFlags::FIXED,
        codepage: 1252,
        root_flag: None,
        record_offset: Some(4),
        name: "Type".to_owned(),
    },
    Column {
        table_object_id: 2,
        column_id: 3,
        column_type: DataType::Long,
        length: 4,
        flags: ColumnFlags::FIXED,
        codepage: 1252,
        root_flag: None,
        record_offset: Some(4),
        name: "Id".to_owned(),
    },
    Column {
        table_object_id: 2,
        column_id: 4,
        column_type: DataType::Long,
        length: 4,
        flags: ColumnFlags::FIXED,
        codepage: 1252,
        root_flag: None,
        record_offset: Some(4),
        name: "ColtypOrPgnoFDP".to_owned(),
    },
    Column {
        table_object_id: 2,
        column_id: 5,
        column_type: DataType::Long,
        length: 4,
        flags: ColumnFlags::FIXED,
        codepage: 1252,
        root_flag: None,
        record_offset: Some(4),
        name: "SpaceUsage".to_owned(),
    },
    Column {
        table_object_id: 2,
        column_id: 6,
        column_type: DataType::Long,
        length: 4,
        flags: ColumnFlags::FIXED,
        codepage: 1252,
        root_flag: None,
        record_offset: Some(4),
        name: "Flags".to_owned(),
    },
    Column {
        table_object_id: 2,
        column_id: 7,
        column_type: DataType::Long,
        length: 4,
        flags: ColumnFlags::FIXED,
        codepage: 1252,
        root_flag: None,
        record_offset: Some(4),
        name: "PagesOrLocale".to_owned(),
    },
    Column {
        table_object_id: 2,
        column_id: 8,
        column_type: DataType::Bit,
        length: 1,
        flags: ColumnFlags::empty(),
        codepage: 1252,
        root_flag: None,
        record_offset: Some(4),
        name: "RootFlag".to_owned(),
    },
    Column {
        table_object_id: 2,
        column_id: 9,
        column_type: DataType::Short,
        length: 2,
        flags: ColumnFlags::empty(),
        codepage: 1252,
        root_flag: None,
        record_offset: Some(4),
        name: "RecordOffset".to_owned(),
    },
    Column {
        table_object_id: 2,
        column_id: 128,
        column_type: DataType::Text,
        length: 255,
        flags: ColumnFlags::FIXED,
        codepage: 1252,
        root_flag: None,
        record_offset: None,
        name: "Name".to_owned(),
    },
]);


fn reference_bytes_to_value_number(bytes: &[u8]) -> usize {
    let mut page_number = 0;
    // little-endian encoding, so do Horner scheme in reverse
    for b in bytes.iter().rev() {
        page_number *= 256;
        page_number += usize::from(*b);
    }
    page_number
}


#[instrument(skip(reader, header))]
pub fn decode_row<R: Read + Seek>(
    reader: &mut R,
    header: &Header,
    row_data: &[u8],
    columns: &[Column],
    page_size: u32,
    large_value_page_number: Option<u64>,
) -> Result<BTreeMap<i32, Value>, ReadError> {
    let mut sorted_columns: Vec<&Column> = columns.iter().collect();
    sorted_columns.sort_unstable_by_key(|c| c.column_id);

    let fixed_columns: Vec<&Column> = sorted_columns.iter()
        .take_while(|c| c.column_id < 128)
        .map(|c| *c)
        .collect();
    let variable_columns: BTreeMap<i32, &Column> = sorted_columns.iter()
        .skip_while(|c| c.column_id < 128)
        .take_while(|c| c.column_id < 256)
        .map(|c| (c.column_id, *c))
        .collect();
    let tagged_columns: BTreeMap<i32, &Column> = sorted_columns.iter()
        .skip_while(|c| c.column_id < 256)
        .map(|c| (c.column_id, *c))
        .collect();

    let (last_fixed_data_column, last_variable_data_column, end_fixed_values_offset) = {
        let mut read = LittleEndianRead::new(Cursor::new(row_data));

        let lfdc: usize = read.read_u8()?.into();
        let lvdc: usize = read.read_u8()?.into();
        let efvo: usize = read.read_u16()?.into();

        (lfdc, lvdc, efvo)
    };

    let variable_column_count = if last_variable_data_column >= 128 {
        last_variable_data_column + 1 - 128
    } else {
        0
    };

    // ceil(last_fixed_data_column / 8.0)
    let nullity_byte_count = (last_fixed_data_column + 7) / 8;

    let fixed_start = 4;
    let nullity_start = end_fixed_values_offset - nullity_byte_count;
    let variable_start = end_fixed_values_offset;

    let fixed_slice = &row_data[fixed_start..nullity_start];
    let nullity_slice = &row_data[nullity_start..variable_start];
    let variable_and_tagged_slice = &row_data[variable_start..];

    let mut ret = BTreeMap::new();

    let mut fixed_read = LittleEndianRead::new(Cursor::new(fixed_slice));
    for fixed_column in fixed_columns.iter().take(last_fixed_data_column) {
        let column_id = fixed_column.column_id;

        let value = match fixed_column.column_type {
            DataType::Nil|DataType::LongBinary|DataType::LongText|DataType::SuperLongValue|DataType::Other(_) => {
                return Err(ReadError::UnexpectedFixedColumnDataType {
                    table_id: fixed_column.table_object_id,
                    column_id: fixed_column.column_id,
                    data_type: fixed_column.column_type,
                });
            },
            DataType::Bit => {
                let inner_value = fixed_read.read_u8()?;
                Data::Bit(Bit(inner_value))
            },
            DataType::UnsignedByte => {
                let inner_value = fixed_read.read_u8()?;
                Data::UnsignedByte(inner_value)
            },
            DataType::Short => {
                let inner_value = fixed_read.read_i16()?;
                Data::Short(inner_value)
            },
            DataType::Long => {
                let inner_value = fixed_read.read_i32()?;
                Data::Long(inner_value)
            },
            DataType::Currency => {
                let inner_value = fixed_read.read_i64()?;
                Data::Currency(inner_value)
            },
            DataType::IeeeSingle => {
                let inner_value = fixed_read.read_f32()?;
                Data::IeeeSingle(inner_value)
            },
            DataType::IeeeDouble => {
                let inner_value = fixed_read.read_f64()?;
                Data::IeeeDouble(inner_value)
            },
            DataType::DateTime => {
                let inner_value = fixed_read.read_i64()?;
                Data::DateTime(inner_value)
            },
            DataType::Binary => {
                let field_length: usize = fixed_column.length.try_into().unwrap();
                let mut inner_value = vec![0u8; field_length];
                fixed_read.read_exact(&mut inner_value)?;
                Data::Binary(inner_value)
            },
            DataType::Text => {
                let field_length: usize = fixed_column.length.try_into().unwrap();
                let mut bytes = vec![0u8; field_length];
                fixed_read.read_exact(&mut bytes)?;

                let string = decode_string(&bytes, fixed_column.codepage);
                Data::Text(string)
            },
            DataType::UnsignedLong => {
                let inner_value = fixed_read.read_u32()?;
                Data::UnsignedLong(inner_value)
            },
            DataType::LongLong => {
                let inner_value = fixed_read.read_i64()?;
                Data::LongLong(inner_value)
            },
            DataType::Guid => {
                let mut guid_bytes = [0u8; 16];
                fixed_read.read_exact(&mut guid_bytes)?;
                let inner_value = Uuid::from_bytes_le(guid_bytes);
                Data::Guid(inner_value)
            },
            DataType::UnsignedShort => {
                let inner_value = fixed_read.read_u16()?;
                Data::UnsignedShort(inner_value)
            },
        };

        // check nullity
        let is_null = {
            let column_index: usize = (column_id - 1).try_into().unwrap();
            let byte_index = column_index / 8;
            let bit_index = column_index % 8;

            nullity_slice
                .get(byte_index)
                .map(|b| *b & (1 << bit_index) != 0)
                .unwrap_or(false)
        };

        let nullified_value = if is_null {
            Data::Nil
        } else {
            value
        };
        ret.insert(fixed_column.column_id, Value::Simple(nullified_value));
    }

    let (variable_offsets_slice, variable_and_tagged_data_slice) = variable_and_tagged_slice.split_at(2*variable_column_count);

    let mut variable_offsets_read = LittleEndianRead::new(Cursor::new(variable_offsets_slice));

    // read the variable offsets
    let mut variable_offsets = vec![0; variable_column_count + 1];
    for i in 0..variable_column_count {
        let column_offset = variable_offsets_read.read_u16()?;
        variable_offsets[i + 1] = column_offset;
    }

    for (i, pair) in variable_offsets.windows(2).enumerate() {
        let column_id: i32 = (128 + i).try_into().unwrap();

        let (begin_raw, end_raw) = (pair[0], pair[1]);
        if end_raw & (1 << 15) != 0 {
            // topmost bit set => no value for this column
            continue;
        }

        let begin: usize = (begin_raw & 0b0111_1111_1111_1111).into();
        let end: usize = (end_raw & 0b0111_1111_1111_1111).into();

        let data_slice = &variable_and_tagged_data_slice[begin..end];

        let column_def = match variable_columns.get(&column_id) {
            Some(cd) => cd,
            None => {
                // uhh, just pretend it's bytes
                ret.insert(column_id, Value::Simple(Data::Binary(data_slice.to_vec())));
                continue;
            },
        };

        let data = match column_def.column_type {
            DataType::Nil|DataType::Bit|DataType::UnsignedByte|DataType::Short
                    |DataType::Long|DataType::Currency|DataType::IeeeSingle|DataType::IeeeDouble
                    |DataType::DateTime|DataType::LongBinary|DataType::LongText|DataType::SuperLongValue
                    |DataType::UnsignedLong|DataType::LongLong|DataType::Guid|DataType::UnsignedShort
                    |DataType::Other(_) => {
                return Err(ReadError::UnexpectedFixedColumnDataType {
                    table_id: column_def.table_object_id,
                    column_id: column_def.column_id,
                    data_type: column_def.column_type,
                });
            },
            DataType::Text => {
                let string = decode_string(data_slice, column_def.codepage);
                Data::Text(string)
            },
            DataType::Binary => {
                Data::Binary(data_slice.to_vec())
            },
        };
        ret.insert(column_id, Value::Simple(data));
    }

    let tagged_start = usize::from(*variable_offsets.last().unwrap());
    let tagged_data_slice = &variable_and_tagged_data_slice[tagged_start..];
    if tagged_data_slice.len() > 0 {
        let mut tags_offsets_flags = Vec::new();

        {
            let mut min_tagged_data_offset = u64::MAX;
            let mut tagged_read = LittleEndianRead::new(Cursor::new(tagged_data_slice));
            while tagged_read.stream_position().unwrap() < min_tagged_data_offset {
                let tag = tagged_read.read_u16()?;
                let offset_and_flags = tagged_read.read_u16()?;

                let (offset, flags) = if page_size <= MAX_SIZE_SMALL_PAGE {
                    (
                        offset_and_flags & 0b0001_1111_1111_1111,
                        SmallTagFlags::from_bits_retain(offset_and_flags & 0b1110_0000_0000_0000),
                    )
                } else {
                    (
                        offset_and_flags & 0b0111_1111_1111_1111,
                        // on large pages, always set the extended header flag
                        SmallTagFlags::HAS_EXTENDED_FLAGS | SmallTagFlags::from_bits_retain(offset_and_flags & 0b1000_0000_0000_0000),
                    )
                };
                tags_offsets_flags.push((tag, offset, flags));
                min_tagged_data_offset = min_tagged_data_offset.min(offset.into());
            }
        }

        for (i, &(tag, offset, small_flags)) in tags_offsets_flags.iter().enumerate() {
            let begin = usize::from(offset);
            let mut item_slice = if i < tags_offsets_flags.len() - 1 {
                let next_begin: usize = usize::from(tags_offsets_flags[i+1].1);
                &tagged_data_slice[begin..next_begin]
            } else {
                &tagged_data_slice[begin..]
            };

            let column_number: i32 = tag.into();
            let Some(column) = tagged_columns.get(&column_number) else { continue };

            let flags = if small_flags.contains(SmallTagFlags::HAS_EXTENDED_FLAGS) {
                // beautiful
                let mut big_flags = TagFlags::from_bits_retain(item_slice[0]);

                // the small flags can only contain DERIVED
                if small_flags.contains(SmallTagFlags::DERIVED) {
                    // pass it on to the big flags
                    big_flags |= TagFlags::DERIVED;
                }

                // skip the first byte
                item_slice = &item_slice[1..];
                big_flags
            } else {
                TagFlags::from_small(small_flags)
            };

            let mut multi_slices;
            let slices = if flags.contains(TagFlags::TWO_VALUES) {
                let first_value_length = usize::from(item_slice[0]);
                let (first_slice, second_slice) = item_slice[1..].split_at(first_value_length);
                &[first_slice, second_slice][..]
            } else if flags.contains(TagFlags::MULTI_VALUES) {
                let first_value_offset = u16::from_le_bytes(item_slice[0..2].try_into().unwrap());
                let offsets_slice = &item_slice[..first_value_offset.into()];

                let mut offsets = Vec::with_capacity(offsets_slice.len() / 2 + 1);
                for chunk in offsets_slice.chunks(2) {
                    let offset: usize = u16::from_le_bytes(chunk.try_into().unwrap()).into();
                    offsets.push(offset);
                }

                // append the length of the slice as the final "offset"
                offsets.push(item_slice.len());

                multi_slices = Vec::with_capacity(offsets.len() - 1);
                for pair in offsets.windows(2) {
                    let (begin, end) = (pair[0], pair[1]);
                    multi_slices.push(&item_slice[begin..end]);
                }
                multi_slices.as_slice()
            } else {
                &[item_slice][..]
            };

            let mut values = Vec::with_capacity(slices.len());
            for &value_slice in slices {
                match column.column_type {
                    DataType::Long => {
                        let inner_value = i32::from_le_bytes(value_slice.try_into().unwrap());
                        values.push(Data::Long(inner_value));
                    },
                    DataType::Currency => {
                        let inner_value = i64::from_le_bytes(value_slice.try_into().unwrap());
                        values.push(Data::Currency(inner_value));
                    },
                    DataType::LongText => {
                        if flags.contains(TagFlags::SEPARATED) {
                            // the data is stored in a different page
                            let Some(sep_page_number) = large_value_page_number else {
                                return Err(ReadError::SeparatedValueWithoutLongValueInfo)
                            };
                            let value_number = reference_bytes_to_value_number(value_slice);
                            let mut separated_values = Vec::new();
                            let mut skip_index = 0;
                            read_data_from_tree(
                                reader,
                                header,
                                sep_page_number,
                                value_number,
                                1,
                                &mut separated_values,
                                &mut skip_index,
                            )?;
                            for separated_value in separated_values {
                                let separated_string = decode_string(&separated_value, column.codepage);
                                values.push(Data::LongText(separated_string));
                            }
                        } else {
                            let inner_value = decode_string(value_slice, column.codepage);
                            values.push(Data::LongText(inner_value));
                        }
                    },
                    DataType::LongBinary => {
                        if flags.contains(TagFlags::SEPARATED) {
                            let Some(sep_page_number) = large_value_page_number else {
                                return Err(ReadError::SeparatedValueWithoutLongValueInfo)
                            };
                            let value_number = reference_bytes_to_value_number(value_slice);
                            let mut separated_values = Vec::new();
                            let mut skip_index = 0;
                            read_data_from_tree(
                                reader,
                                header,
                                sep_page_number,
                                value_number,
                                1,
                                &mut separated_values,
                                &mut skip_index,
                            )?;
                            for separated_value in separated_values {
                                values.push(Data::LongBinary(separated_value));
                            }
                        } else {
                            let inner_value = value_slice.to_vec();
                            values.push(Data::LongBinary(inner_value));
                        }
                    },
                    other => {
                        return Err(ReadError::UnexpectedTaggedColumnDataType {
                            table_id: column.table_object_id,
                            column_id: column.column_id,
                            data_type: other,
                        });
                    },
                }
            }

            if flags.contains(TagFlags::SEPARATED) && column.column_type != DataType::LongText && column.column_type != DataType::LongBinary {
                panic!("unexpected data type for SEPARATED");
            }

            if values.len() == 1 {
                ret.insert(column.column_id, Value::Complex {
                    data: values.swap_remove(0),
                    flags,
                });
            } else {
                ret.insert(column.column_id, Value::Multiple {
                    values,
                    flags,
                });
            }
        }
    }

    Ok(ret)
}

#[instrument]
fn decode_string(bytes: &[u8], codepage: i32) -> String {
    if codepage == 1200 {
        // UTF-16LE
        let mut words = Vec::with_capacity(bytes.len() / 2);
        for chunk in bytes.chunks_exact(2) {
            let word = u16::from_le_bytes(chunk.try_into().unwrap());
            words.push(word);
        }
        return String::from_utf16(&words).unwrap();
    }

    if codepage < 0 || codepage > 0xFFFF {
        panic!("cannot handle codepage {} (0x{:X})", codepage, codepage as u32);
    }
    let Some(encoder) = codepage::to_encoding(codepage.try_into().unwrap()) else {
        panic!("cannot find decoder for codepage {} (0x{:X})", codepage, codepage as u32);
    };
    let mut string = String::with_capacity(1024);

    let mut decoder = encoder.new_decoder_with_bom_removal();
    let mut offset = 0;
    loop {
        let (res, crunched) = decoder.decode_to_string_without_replacement(&bytes[offset..], &mut string, true);
        offset += crunched;
        match res {
            DecoderResult::InputEmpty => break,
            DecoderResult::OutputFull => {
                string.reserve(512);
            },
            DecoderResult::Malformed(_, _) => {
                panic!("effed up decoding of {0:?} in codepage {1} (0x{1:X})", bytes, codepage);
            },
        }
    }
    string
}

fn get_name_to_column(columns: &[Column]) -> BTreeMap<&str, &Column> {
    columns
        .iter()
        .map(|col| (col.name.as_str(), col))
        .collect()
}

#[instrument(skip(reader, header), fields(header.page_number, header.version, header.revision))]
pub fn read_table_from_pages<R: Read + Seek>(
    reader: &mut R,
    header: &Header,
    page_number: u64,
    columns: &[Column],
    large_value_page_number: Option<u64>,
) -> Result<Vec<BTreeMap<i32, Value>>, ReadError> {
    let mut raw_rows = Vec::new();
    let mut skip_index = 0;
    read_data_from_tree(reader, header, page_number, 0, usize::MAX, &mut raw_rows, &mut skip_index)?;

    let mut rows = Vec::with_capacity(raw_rows.len());
    for raw_row in raw_rows {
        let row = decode_row(reader, header, &raw_row, columns, header.page_size, large_value_page_number)?;
        trace!(?row);
        rows.push(row);
    }

    Ok(rows)
}

#[instrument]
pub fn collect_tables(rows: &[BTreeMap<i32, Value>], metadata_columns: &[Column]) -> Result<Vec<Table>, ReadError> {
    let name_to_column = get_name_to_column(metadata_columns);

    let mut table_number_to_header: BTreeMap<i32, TableHeader> = BTreeMap::new();
    let mut table_number_to_columns: BTreeMap<i32, Vec<Column>> = BTreeMap::new();
    let mut table_number_to_indexes: BTreeMap<i32, Vec<Index>> = BTreeMap::new();
    let mut table_number_to_long_value: BTreeMap<i32, LongValueInfo> = BTreeMap::new();

    for row in rows {
        let type_value_i16 = *get_value!(@required, name_to_column, row, "Type", Short);
        let type_value = ObjectType::from_base_type(type_value_i16);
        match type_value {
            ObjectType::Table => {
                let header = TableHeader::try_from_metadata(metadata_columns, row)?;
                table_number_to_header.insert(header.table_object_id, header);
            },
            ObjectType::Column => {
                let column = Column::try_from_metadata(metadata_columns, row)?;
                table_number_to_columns
                    .entry(column.table_object_id)
                    .or_insert_with(|| Vec::new())
                    .push(column);
            },
            ObjectType::Index => {
                let index = Index::try_from_metadata(metadata_columns, row)?;
                table_number_to_indexes
                    .entry(index.table_object_id)
                    .or_insert_with(|| Vec::new())
                    .push(index);
            },
            ObjectType::LongValue => {
                let long_value = LongValueInfo::try_from_metadata(metadata_columns, row)?;
                table_number_to_long_value.insert(long_value.table_object_id, long_value);
            },
            _ => {
                // currently unhandled...
            },
        }
    }

    for columns in table_number_to_columns.values_mut() {
        columns.sort_unstable_by_key(|c| c.column_id);
    }
    for indexes in table_number_to_indexes.values_mut() {
        indexes.sort_unstable_by_key(|c| c.index_id);
    }

    let mut tables = Vec::with_capacity(table_number_to_header.len());
    for (_, header) in table_number_to_header {
        let columns = table_number_to_columns.remove(&header.table_object_id)
            .unwrap_or_else(|| Vec::with_capacity(0));
        let indexes = table_number_to_indexes.remove(&header.table_object_id)
            .unwrap_or_else(|| Vec::with_capacity(0));
        let long_value = table_number_to_long_value.remove(&header.table_object_id);
        tables.push(Table {
            header,
            columns,
            indexes,
            long_value,
        });
    }

    Ok(tables)
}
