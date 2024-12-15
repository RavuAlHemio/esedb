use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, spanned::Spanned, Data, DeriveInput, Error, Ident};


#[proc_macro_derive(ReadFromAndWriteToBytes)]
pub fn derive_read_write_binary(input_tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input_tokens as DeriveInput);

    let name = input.ident;

    match &input.data {
        Data::Struct(data_struct) => {
            // read/write the fields in turn
            let mut reads = Vec::with_capacity(data_struct.fields.len());
            let mut assemble_fields = Vec::with_capacity(data_struct.fields.len());
            let mut writes = Vec::with_capacity(data_struct.fields.len());
            for field in data_struct.fields.iter() {
                let Some(field_name) = &field.ident else { continue };
                reads.push(quote! { let #field_name = crate::byte_io::ReadFromBytes::read_from_bytes(reader)?; });
                assemble_fields.push(quote! { #field_name , });
                writes.push(quote! { crate::byte_io::WriteToBytes::write_to_bytes(&self. #field_name , writer)?; });
            }

            let output = quote! {
                impl crate::byte_io::ReadFromBytes for #name {
                    type Error = ::std::io::Error;

                    fn read_from_bytes<R: crate::byte_io::ByteRead>(reader: &mut R) -> Result<Self, Self::Error> {
                        #(
                            #reads
                        )*
                        Ok(Self {
                            #(
                                #assemble_fields
                            )*
                        })
                    }
                }

                impl crate::byte_io::WriteToBytes for #name {
                    type Error = ::std::io::Error;

                    fn write_to_bytes<W: crate::byte_io::ByteWrite>(&self, writer: &mut W) -> Result<(), Self::Error> {
                        #(
                            #writes
                        )*
                        Ok(())
                    }
                }
            };
            output.into()
        },
        Data::Enum(_data_enum) => {
            // do we have a "repr" or "from_to_other" attribute?
            let mut must_try = false;
            let mut base_type_opt = None;
            for attr in &input.attrs {
                if attr.path().is_ident("repr") {
                    let _ = attr.parse_nested_meta(|meta| {
                        if let Some(i) = meta.path.get_ident() {
                            if i == "u8" || i == "u16" || i == "u32" || i == "u64" || i == "u128" || i == "usize"
                                    || i == "i8" || i == "i16" || i == "i32" || i == "i64" || i == "i128" || i == "isize" {
                                must_try = true;
                                base_type_opt = Some(i.to_string());
                                return Ok(());
                            }
                        }
                        panic!("unknown repr");
                    });
                } else if attr.path().is_ident("from_to_other") {
                    let _ = attr.parse_nested_meta(|meta| {
                        if meta.path.is_ident("base_type") {
                            let value = meta.value()?;
                            let value_parsed: Ident = value.parse()?;
                            must_try = false;
                            base_type_opt = Some(value_parsed.to_string());
                        }
                        Ok(())
                    });
                }

                if base_type_opt.is_some() {
                    break;
                }
            }

            let Some(base_type) = base_type_opt else {
                return Error::new(name.span(), "ReadFromAndWriteToBytes cannot identify base type")
                    .to_compile_error()
                    .into();
            };
            let read_base_type_string = format!("read_{}", base_type);
            let read_base_type_ident = Ident::new(&read_base_type_string, name.span());
            let write_base_type_string = format!("write_{}", base_type);
            let write_base_type_ident = Ident::new(&write_base_type_string, name.span());

            let output = if must_try {
                // FromToRepr
                quote! {
                    impl crate::byte_io::ReadFromBytes for #name {
                        type Error = crate::error::IoOrInvalidValueError;

                        fn read_from_bytes<R: crate::byte_io::ByteRead>(reader: &mut R) -> Result<Self, Self::Error> {
                            let value = reader. #read_base_type_string ()?;
                            match Self::try_from_repr(value) {
                                Some(v) => Ok(v),
                                None => Err(Self::Error::InvalidValue),
                            }
                        }
                    }

                    impl crate::byte_io::WriteToBytes for #name {
                        type Error = ::std::io::Error;

                        fn write_to_bytes<W: crate::byte_io::ByteWrite>(&self, writer: &mut W) -> Result<(), Self::Error> {
                            let value = self.into_repr();
                            writer. #write_base_type_string (value)
                        }
                    }
                }
            } else {
                // from_to_other
                quote! {
                    impl crate::byte_io::ReadFromBytes for #name {
                        type Error = ::std::io::Error;

                        fn read_from_bytes<R: crate::byte_io::ByteRead>(reader: &mut R) -> Result<Self, Self::Error> {
                            let value = reader. #read_base_type_ident ()?;
                            Ok(Self::from_base_type(value))
                        }
                    }

                    impl crate::byte_io::WriteToBytes for #name {
                        type Error = ::std::io::Error;

                        fn write_to_bytes<W: crate::byte_io::ByteWrite>(&self, writer: &mut W) -> Result<(), Self::Error> {
                            let value = self.to_base_type();
                            writer. #write_base_type_ident (value)
                        }
                    }
                }
            };
            output.into()
        },
        Data::Union(data_union) => {
            Error::new(data_union.union_token.span(), "cannot derive ReadFromAndWriteToBytes for unions")
                .to_compile_error()
                .into()
        },
    }
}
