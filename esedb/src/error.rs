use std::borrow::Cow;
use std::fmt;
use std::io;


#[derive(Debug)]
pub enum ReadError {
    Io(io::Error),
    WrongHeaderSignature { expected: u32, read: u32 },
    WrongHeaderChecksum { calculated: u32, read: u32 },
    HeaderLongerThanPage { header_length: usize, page_size: usize },
    PageSizeNotDivisibleBy4 { page_size: usize },
    Page0,
    UnknownFormatVariant,
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
