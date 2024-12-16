/// Like [`bitflags::bitflags!`], but additionally generates implementations for
/// [`crate::byte_io::ReadFromBytes`] and [`crate::byte_io::WriteToBytes`].
#[macro_export]
macro_rules! bitflags_read_write_bytes {
    {
        $( #[$attribute:meta] )*
        $vis:vis struct $name:ident : $base_type:ty {
            $(const $field_name:ident = $value:expr;)*
        }
    } => {
        ::bitflags::bitflags! {
            $( #[$attribute] )*
            $vis struct $name : $base_type {
                $(const $field_name = $value;)*
            }
        }
        impl crate::byte_io::ReadFromBytes for $name {
            type Error = ::std::io::Error;
            fn read_from_bytes<R: crate::byte_io::ByteRead>(reader: &mut R) -> Result<Self, Self::Error> {
                let value: $base_type = crate::byte_io::ReadFromBytes::read_from_bytes(reader)?;
                Ok(Self::from_bits_retain(value))
            }
        }
        impl crate::byte_io::WriteToBytes for $name {
            type Error = ::std::io::Error;
            fn write_to_bytes<W: crate::byte_io::ByteWrite>(&self, writer: &mut W) -> Result<(), Self::Error> {
                let bits = self.bits();
                crate::byte_io::WriteToBytes::write_to_bytes(&bits, writer)
            }
        }
    };
}
