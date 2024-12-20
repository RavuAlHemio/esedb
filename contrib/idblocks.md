# ID Blocks

Active Directory stores references to object and attribute classes using a compressed syntax.

Each such reference is an unsigned 32-bit integer 0xPPPPIIII where 0xPPPP encodes a prefix and
0xIIII an item.

To translate from such a reference to an OID, the following steps are taken:

1. Look up 0xPPPP in the prefix table to obtain the OID prefix.

2. Append 0xIIII as an additional arc to the OID prefix from step 1.

For example, if the prefix 0x0001 maps to the OID prefix `2.5.6` and a reference 0x00010002 is
encountered, the reference is interpreted as the OID `2.5.6.2`.

Note that an item is always an immediate child of the OID prefix. An existing prefix cannot be used
if the value is two or more layers below the prefix.

Prefixes are sourced from a built-in table and a custom table in the directory.

## Built-In Prefixes

| encoding   | OID subtree                | A | O | S | BER-encoded subtree           |
| ---------- | -------------------------- | - | - | - | ----------------------------- |
| 0x0000xxxx | 2.5.4.x                    | A |   |   | 55 04                         |
| 0x0001xxxx | 2.5.6.x                    |   | O |   | 55 06                         |
| 0x0002xxxx | 1.2.840.113556.1.2.x       | A |   |   | 2A 86 48 86 F7 14 01 02       |
| 0x0003xxxx | 1.2.840.113556.1.3.x       |   | O |   | 2A 86 48 86 F7 14 01 03       |
| 0x0004xxxx | 2.16.840.1.101.2.2.1.x     |   |   |   | 60 86 48 01 65 02 02 01       |
| 0x0005xxxx | 2.16.840.1.101.2.2.3.x     |   |   |   | 60 86 48 01 65 02 02 03       |
| 0x0006xxxx | 2.16.840.1.101.2.1.5.x     |   |   |   | 60 86 48 01 65 02 01 05       |
| 0x0007xxxx | 2.16.840.1.101.2.1.4.x     |   |   |   | 60 86 48 01 65 02 01 04       |
| 0x0008xxxx | 2.5.5.x                    |   |   | S | 55 05                         |
| 0x0009xxxx | 1.2.840.113556.1.4.x       | A |   |   | 2A 86 48 86 F7 14 01 04       |
| 0x000Axxxx | 1.2.840.113556.1.5.x       |   | O |   | 2A 86 48 86 F7 14 01 05       |
| 0x000Bxxxx | 1.2.840.113556.1.4.260.x   |   |   |   | 2A 86 48 86 F7 14 01 04 82 04 |
| 0x000Cxxxx | 1.2.840.113556.1.5.56.x    |   |   |   | 2A 86 48 86 F7 14 01 05 38    |
| 0x000Dxxxx | 1.2.840.113556.1.4.262.x   |   |   |   | 2A 86 48 86 F7 14 01 04 82 06 |
| 0x000Exxxx | 1.2.840.113556.1.5.57.x    |   |   |   | 2A 86 48 86 F7 14 01 05 39    |
| 0x000Fxxxx | 1.2.840.113556.1.4.263.x   |   |   |   | 2A 86 48 86 F7 14 01 04 82 07 |
| 0x0010xxxx | 1.2.840.113556.1.5.58.x    |   |   |   | 2A 86 48 86 F7 14 01 05 3A    |
| 0x0011xxxx | 1.2.840.113556.1.5.73.x    |   |   |   | 2A 86 48 86 F7 14 01 05 49    |
| 0x0012xxxx | 1.2.840.113556.1.4.305.x   |   |   |   | 2A 86 48 86 F7 14 01 04 82 31 |
| 0x0013xxxx | 0.9.2342.19200300.100.x    |   |   |   | 09 92 26 89 93 F2 2C 64       |
| 0x0014xxxx | 2.16.840.1.113730.3.x      | A |   |   | 60 86 48 01 86 F8 42 03       |
| 0x0015xxxx | 0.9.2342.19200300.100.1.x  | A |   |   | 09 92 26 89 93 F2 2C 64 01    |
| 0x0016xxxx | 2.16.840.1.113730.3.1.x    | A |   |   | 60 86 48 01 86 F8 42 03 01    |
| 0x0017xxxx | 1.2.840.113556.1.5.7000.x  |   | O |   | 2A 86 48 86 F7 14 01 05 B6 58 |
| 0x0018xxxx | 2.5.21.x                   | A |   |   | 55 15                         |
| 0x0019xxxx | 2.5.18.x                   | A |   |   | 55 12                         |
| 0x001Axxxx | 2.5.20.x                   |   | O |   | 55 14                         |
| 0x001Bxxxx | 1.3.6.1.4.1.1466.101.119.x | A | O |   | 2B 06 01 04 01 8B 3A 65 77    |
| 0x001Cxxxx | 2.16.840.1.113730.3.2.x    |   | O |   | 60 86 48 01 86 F8 42 03 02    |
| 0x001Dxxxx | 1.3.6.1.4.1.250.1.x        | A |   |   | 2B 06 01 04 01 81 7A 01       |
| 0x001Exxxx | 1.2.840.113549.1.9.x       | A |   |   | 2A 86 48 86 F7 0D 01 09       |
| 0x001Fxxxx | 0.9.2342.19200300.100.4.x  |   | O |   | 09 92 26 89 93 F2 2C 64 04    |
| 0x0020xxxx | 1.2.840.113556.1.6.23.x    |   |   |   | 2A 86 48 86 F7 14 01 06 17    |
| 0x0021xxxx | 1.2.840.113556.1.6.18.1.x  |   |   |   | 2A 86 48 86 F7 14 01 06 12 01 |
| 0x0022xxxx | 1.2.840.113556.1.6.18.2.x  |   |   |   | 2A 86 48 86 F7 14 01 06 12 02 |
| 0x0023xxxx | 1.2.840.113556.1.6.13.3.x  |   |   |   | 2A 86 48 86 F7 14 01 06 0D 03 |
| 0x0024xxxx | 1.2.840.113556.1.6.13.4.x  |   |   |   | 2A 86 48 86 F7 14 01 06 0D 04 |
| 0x0025xxxx | 1.3.6.1.1.1.1.x            |   |   |   | 2B 06 01 01 01 01             |
| 0x0026xxxx | 1.3.6.1.1.1.2.x            |   |   |   | 2B 06 01 01 01 02             |

Usage in the default AD schema:

* A: attribute IDs
* O: object class `Governs-Id`s
* S: data syntaxes

The preparation logic can be found in `ntdsai.dll` in a function named `InitPrefixTable`.

## Custom Prefixes

Whenever the schema is modified to include an object class or attribute outside of one of the
existing prefixes, a new custom prefix is defined. These prefixes are stored in the `prefixMap`
attribute of the `Schema` object (`cn=Schema,cn=Configuration,...`, the one with `objectClass=dMD`).

The structure of `prefixMap` is as follows, with all integers in little-endian byte order:

```
number_of_entries: u32,
total_length_in_bytes_of_prefixmap_structure: u32,
for each entry:
    prefix: u16,
    oid_ber_length: u16
    oid_ber: [u8; oid_ber_length]
```

OIDs are always stored in their BER encoding, e.g. `55 04` for 2.5.4.

An annotated example:

```
02 00 00 00 // number of entries
25 00 00 00 // length of the whole structure (yup, 37 bytes)
    3B 64   // prefix
    0A 00   // OID length
        2B 06 01 04 01 82 8B 13 01 45 // BER-encoded OID
    3D 56   // prefix
    0B 00   // OID length
        2B 06 01 04 01 82 8B 13 01 83 24 // BER-encoded OID
```

This defines the following prefixes:

| encoding   | OID subtree               | BER-encoded subtree              |
| ---------- | ------------------------- | -------------------------------- |
| 0x643Bxxxx | 1.3.6.1.4.1.34195.1.69.x  | 2B 06 01 04 01 82 8B 13 01 45    |
| 0x563Dxxxx | 1.3.6.1.4.1.34195.1.420.x | 2B 06 01 04 01 82 8B 13 01 83 24 |

An attribute 1681588644 (0x643B01A4) would then have the OID 1.3.6.1.4.1.34195.1.69.420, while
an attribute 1446838341 (0x563D0045) would have the OID 1.3.6.1.4.1.34195.1.420.69.

The loading logic is in `ntdsai.dll` in a function named `InitPrefixTable2`.
