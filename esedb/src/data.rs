use from_to_repr::from_to_other;
use uuid::Uuid;


#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Bit(pub u8);
impl From<Bit> for bool {
    fn from(value: Bit) -> Self { value.0 != 0x00 }
}
impl From<bool> for Bit {
    fn from(value: bool) -> Self {
        // go whole-hog
        Self(if value { 0xFF } else { 0x00 })
    }
}

#[derive(Clone, Copy, Debug)]
#[from_to_other(base_type = i32, derive_compare = "as_int")]
pub enum DataType {
    Nil = 0,
    Bit = 1,
    UnsignedByte = 2,
    Short = 3,
    Long = 4,
    Currency = 5,
    IeeeSingle = 6,
    IeeeDouble = 7,
    DateTime = 8,
    Binary = 9,
    Text = 10,
    LongBinary = 11,
    LongText = 12,
    SuperLongValue = 13,
    UnsignedLong = 14,
    LongLong = 15,
    Guid = 16,
    UnsignedShort = 17,
    Other(i32),
}
impl DataType {
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::Nil => Some(0),
            Self::Bit => Some(1),
            Self::UnsignedByte => Some(1),
            Self::Short => Some(2),
            Self::Long => Some(4),
            Self::Currency => Some(8),
            Self::IeeeSingle => Some(4),
            Self::IeeeDouble => Some(8),
            Self::DateTime => Some(8),
            Self::Binary => None,
            Self::Text => None,
            Self::LongBinary => None,
            Self::LongText => None,
            Self::SuperLongValue => None,
            Self::UnsignedLong => Some(4),
            Self::LongLong => Some(8),
            Self::Guid => Some(16),
            Self::UnsignedShort => Some(2),
            Self::Other(_) => None,
        }
    }
}


#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Data {
    Nil,
    Bit(Bit),
    UnsignedByte(u8),
    Short(i16),
    Long(i32),
    Currency(i64),
    IeeeSingle(f32),
    IeeeDouble(f64),
    DateTime(i64),
    Binary(Vec<u8>),
    Text(String),
    LongBinary(Vec<u8>),
    LongText(String),
    SuperLongValue(Vec<u8>),
    UnsignedLong(u32),
    LongLong(i64),
    Guid(Uuid),
    UnsignedShort(u16),
    Other(i32, Vec<u8>),
}
impl Data {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Nil => DataType::Nil,
            Self::Bit(_) => DataType::Bit,
            Self::UnsignedByte(_) => DataType::UnsignedByte,
            Self::Short(_) => DataType::Short,
            Self::Long(_) => DataType::Long,
            Self::Currency(_) => DataType::Currency,
            Self::IeeeSingle(_) => DataType::IeeeSingle,
            Self::IeeeDouble(_) => DataType::IeeeDouble,
            Self::DateTime(_) => DataType::DateTime,
            Self::Binary(_) => DataType::Binary,
            Self::Text(_) => DataType::Text,
            Self::LongBinary(_) => DataType::LongBinary,
            Self::LongText(_) => DataType::LongText,
            Self::SuperLongValue(_) => DataType::SuperLongValue,
            Self::UnsignedLong(_) => DataType::UnsignedLong,
            Self::LongLong(_) => DataType::LongLong,
            Self::Guid(_) => DataType::Guid,
            Self::UnsignedShort(_) => DataType::UnsignedShort,
            Self::Other(code, _value) => DataType::Other(*code),
        }
    }
}
