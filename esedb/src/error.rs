use std::borrow::Cow;
use std::fmt;
use std::io;

use crate::data::DataType;
use crate::table::ObjectType;


#[derive(Debug)]
pub enum ReadError {
    Io(io::Error),
    WrongHeaderSignature { expected: u32, read: u32 },
    WrongHeaderChecksum { calculated: u32, read: u32 },
    HeaderLongerThanPage { header_length: usize, page_size: usize },
    PageSizeNotDivisibleBy4 { page_size: usize },
    Page0,
    UnknownFormatVariant,
    UnknownPageType,
    UnexpectedFixedColumnDataType { table_id: i32, column_id: i32, data_type: DataType },
    UnexpectedVariableColumnDataType { table_id: i32, column_id: i32, data_type: DataType },
    UnexpectedTaggedColumnDataType { table_id: i32, column_id: i32, data_type: DataType },
    MissingRequiredColumn { name: Cow<'static, str> },
    WrongColumnType { name: Cow<'static, str>, expected: DataType, obtained: DataType },
    WrongObjectType { expected: ObjectType, obtained: ObjectType },
    SeparatedValueWithoutLongValueInfo,
}
impl ReadError {
    #[must_use]
    pub fn ensure_column_type(name: Cow<'static, str>, expected: DataType, obtained: DataType) -> Result<(), Self> {
        if expected == obtained {
            Ok(())
        } else {
            Err(Self::WrongColumnType { name, expected, obtained })
        }
    }

    #[must_use]
    pub fn ensure_object_type(expected: ObjectType, obtained: ObjectType) -> Result<(), Self> {
        if expected == obtained {
            Ok(())
        } else {
            Err(Self::WrongObjectType { expected, obtained })
        }
    }
}
impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e)
                => write!(f, "I/O error: {}", e),
            Self::WrongHeaderSignature { expected, read }
                => write!(f, "wrong header magic (expected 0x{:08X}, read 0x{:08X})", expected, read),
            Self::WrongHeaderChecksum { calculated, read }
                => write!(f, "wrong header checksum (calculated 0x{:08X}, read 0x{:08X})", calculated, read),
            Self::HeaderLongerThanPage { header_length, page_size }
                => write!(f, "header length ({}) greater than page size ({})", header_length, page_size),
            Self::PageSizeNotDivisibleBy4 { page_size }
                => write!(f, "page size ({}) not divisible by 4", page_size),
            Self::Page0
                => write!(f, "page 0 does not exist"),
            Self::UnknownFormatVariant
                => write!(f, "failed to detect format variant"),
            Self::UnknownPageType
                => write!(f, "unknown page type"),
            Self::UnexpectedFixedColumnDataType { table_id, column_id, data_type }
                => write!(f, "unexpected data type {:?} in table {} fixed column {}", data_type, table_id, column_id),
            Self::UnexpectedVariableColumnDataType { table_id, column_id, data_type }
                => write!(f, "unexpected data type {:?} in table {} variable column {}", data_type, table_id, column_id),
            Self::UnexpectedTaggedColumnDataType { table_id, column_id, data_type }
                => write!(f, "unexpected data type {:?} in table {} tagged column {}", data_type, table_id, column_id),
            Self::MissingRequiredColumn { name }
                => write!(f, "missing required column {:?}", &*name),
            Self::WrongColumnType { name, expected, obtained }
                => write!(f, "column {:?} has data type {:?}, expected {:?}", &*name, obtained, expected),
            Self::WrongObjectType { expected, obtained }
                => write!(f, "object has type {:?}, expected {:?}", obtained, expected),
            Self::SeparatedValueWithoutLongValueInfo
                => write!(f, "table contains a separated value but no long value info"),
        }
    }
}
impl std::error::Error for ReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::WrongHeaderSignature { .. } => None,
            Self::WrongHeaderChecksum { .. } => None,
            Self::HeaderLongerThanPage { .. } => None,
            Self::PageSizeNotDivisibleBy4 { .. } => None,
            Self::Page0 => None,
            Self::UnknownFormatVariant => None,
            Self::UnknownPageType => None,
            Self::UnexpectedFixedColumnDataType { .. } => None,
            Self::UnexpectedVariableColumnDataType { .. } => None,
            Self::UnexpectedTaggedColumnDataType { .. } => None,
            Self::MissingRequiredColumn { .. } => None,
            Self::WrongColumnType { .. } => None,
            Self::WrongObjectType { .. } => None,
            Self::SeparatedValueWithoutLongValueInfo => None,
        }
    }
}
impl From<io::Error> for ReadError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}


#[derive(Debug)]
pub enum WriteError {
    Io(io::Error),
    UnsignedOutOfRange { name: Cow<'static, str>, value: u128, min_value: u128, max_value: u128 },
}
impl WriteError {
    pub fn check_unsigned_max(name: &'static str, value: u128, max_value: u128) -> Result<(), Self> {
        if value <= max_value {
            Ok(())
        } else {
            Err(Self::UnsignedOutOfRange { name: Cow::Borrowed(name), value, min_value: 0, max_value })
        }
    }
}
impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e)
                => write!(f, "I/O error: {}", e),
            Self::UnsignedOutOfRange { name, value, min_value, max_value }
                => write!(f, "unsigned value {} of {:?} out of range (minimum {}, maximum {})", value, name, min_value, max_value),
        }
    }
}
impl std::error::Error for WriteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::UnsignedOutOfRange { .. } => None,
        }
    }
}
impl From<io::Error> for WriteError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
