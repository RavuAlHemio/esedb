use std::io::{self, Read, Seek, Write};


pub trait ByteRead {
    fn read_u8(&mut self) -> Result<u8, io::Error>;
    fn read_u16(&mut self) -> Result<u16, io::Error>;
    fn read_u32(&mut self) -> Result<u32, io::Error>;
    fn read_u64(&mut self) -> Result<u64, io::Error>;
    fn read_u128(&mut self) -> Result<u128, io::Error>;
    fn read_usize(&mut self) -> Result<usize, io::Error>;
    fn read_i8(&mut self) -> Result<i8, io::Error>;
    fn read_i16(&mut self) -> Result<i16, io::Error>;
    fn read_i32(&mut self) -> Result<i32, io::Error>;
    fn read_i64(&mut self) -> Result<i64, io::Error>;
    fn read_i128(&mut self) -> Result<i128, io::Error>;
    fn read_isize(&mut self) -> Result<isize, io::Error>;
    fn read_f32(&mut self) -> Result<f32, io::Error>;
    fn read_f64(&mut self) -> Result<f64, io::Error>;
}

pub trait ByteWrite {
    fn write_u8(&mut self, value: u8) -> Result<(), io::Error>;
    fn write_u16(&mut self, value: u16) -> Result<(), io::Error>;
    fn write_u32(&mut self, value: u32) -> Result<(), io::Error>;
    fn write_u64(&mut self, value: u64) -> Result<(), io::Error>;
    fn write_u128(&mut self, value: u128) -> Result<(), io::Error>;
    fn write_usize(&mut self, value: usize) -> Result<(), io::Error>;
    fn write_i8(&mut self, value: i8) -> Result<(), io::Error>;
    fn write_i16(&mut self, value: i16) -> Result<(), io::Error>;
    fn write_i32(&mut self, value: i32) -> Result<(), io::Error>;
    fn write_i64(&mut self, value: i64) -> Result<(), io::Error>;
    fn write_i128(&mut self, value: i128) -> Result<(), io::Error>;
    fn write_isize(&mut self, value: isize) -> Result<(), io::Error>;
    fn write_f32(&mut self, value: f32) -> Result<(), io::Error>;
    fn write_f64(&mut self, value: f64) -> Result<(), io::Error>;
}


pub trait ReadFromBytes {
    type Error;
    fn read_from_bytes<R: ByteRead>(reader: &mut R) -> Result<Self, Self::Error> where Self: Sized;
}


pub trait WriteToBytes {
    type Error;
    fn write_to_bytes<W: ByteWrite>(&self, writer: &mut W) -> Result<(), Self::Error>;
}

macro_rules! impl_read_write_primitive {
    ($type:ty, $read_func:ident, $write_func:ident) => {
        impl ReadFromBytes for $type {
            type Error = std::io::Error;
            fn read_from_bytes<R: ByteRead>(reader: &mut R) -> Result<Self, Self::Error> {
                let value = reader.$read_func()?;
                Ok(value)
            }
        }

        impl WriteToBytes for $type {
            type Error = std::io::Error;
            fn write_to_bytes<W: ByteWrite>(&self, writer: &mut W) -> Result<(), Self::Error> {
                writer.$write_func(*self)
            }
        }
    };
}
impl_read_write_primitive!(u8, read_u8, write_u8);
impl_read_write_primitive!(u16, read_u16, write_u16);
impl_read_write_primitive!(u32, read_u32, write_u32);
impl_read_write_primitive!(u64, read_u64, write_u64);
impl_read_write_primitive!(u128, read_u128, write_u128);
impl_read_write_primitive!(usize, read_usize, write_usize);
impl_read_write_primitive!(i8, read_i8, write_i8);
impl_read_write_primitive!(i16, read_i16, write_i16);
impl_read_write_primitive!(i32, read_i32, write_i32);
impl_read_write_primitive!(i64, read_i64, write_i64);
impl_read_write_primitive!(i128, read_i128, write_i128);
impl_read_write_primitive!(isize, read_isize, write_isize);
impl_read_write_primitive!(f32, read_f32, write_f32);
impl_read_write_primitive!(f64, read_f64, write_f64);


impl<T: ReadFromBytes, const N: usize> ReadFromBytes for [T; N] {
    type Error = <T as ReadFromBytes>::Error;
    fn read_from_bytes<R: ByteRead>(reader: &mut R) -> Result<Self, Self::Error> where Self: Sized {
        let mut vector: Vec<T> = Vec::with_capacity(N);
        for _ in 0..N {
            let value = ReadFromBytes::read_from_bytes(reader)?;
            vector.push(value);
        }
        match vector.try_into() {
            Ok(array) => Ok(array),
            Err(_) => unreachable!(),
        }
    }
}

impl<T: WriteToBytes, const N: usize> WriteToBytes for [T; N] {
    type Error = <T as WriteToBytes>::Error;
    fn write_to_bytes<W: ByteWrite>(&self, writer: &mut W) -> Result<(), Self::Error> {
        for value in self {
            WriteToBytes::write_to_bytes(value, writer)?;
        }
        Ok(())
    }
}

pub struct LittleEndianRead<R: Read> {
    reader: R,
}
impl<R: Read> LittleEndianRead<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
        }
    }
}
impl<R: Read> Read for LittleEndianRead<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.reader.read_vectored(bufs)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.reader.read_to_end(buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        self.reader.read_to_string(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(buf)
    }
}

macro_rules! impl_read {
    ($func:ident, $type:ty) => {
        impl_read!($func, $type, (<$type>::BITS / 8) as usize);
    };
    ($func:ident, $type:ty, $bytes:expr) => {
        fn $func(&mut self) -> Result<$type, io::Error> {
            let mut buf = [0u8; $bytes];
            self.reader.read_exact(&mut buf)?;
            Ok(<$type>::from_le_bytes(buf))
        }
    };
}

impl<R: Read> ByteRead for LittleEndianRead<R> {
    impl_read!(read_u8, u8);
    impl_read!(read_u16, u16);
    impl_read!(read_u32, u32);
    impl_read!(read_u64, u64);
    impl_read!(read_u128, u128);
    impl_read!(read_usize, usize);
    impl_read!(read_i8, i8);
    impl_read!(read_i16, i16);
    impl_read!(read_i32, i32);
    impl_read!(read_i64, i64);
    impl_read!(read_i128, i128);
    impl_read!(read_isize, isize);
    impl_read!(read_f32, f32, 4);
    impl_read!(read_f64, f64, 8);
}

impl<R: Read + Seek> Seek for LittleEndianRead<R> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.reader.seek(pos)
    }
}

pub struct LittleEndianWrite<W: Write> {
    writer: W,
}
impl<W: Write> LittleEndianWrite<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
        }
    }
}

macro_rules! impl_write {
    ($func:ident, $type:ty) => {
        fn $func(&mut self, value: $type) -> Result<(), io::Error> {
            let buf = value.to_le_bytes();
            self.writer.write_all(&buf)
        }
    };
}

impl<W: Write> ByteWrite for LittleEndianWrite<W> {
    impl_write!(write_u8, u8);
    impl_write!(write_u16, u16);
    impl_write!(write_u32, u32);
    impl_write!(write_u64, u64);
    impl_write!(write_u128, u128);
    impl_write!(write_usize, usize);
    impl_write!(write_i8, i8);
    impl_write!(write_i16, i16);
    impl_write!(write_i32, i32);
    impl_write!(write_i64, i64);
    impl_write!(write_i128, i128);
    impl_write!(write_isize, isize);
    impl_write!(write_f32, f32);
    impl_write!(write_f64, f64);
}
