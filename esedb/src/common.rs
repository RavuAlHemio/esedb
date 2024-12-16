use esedb_macros::ReadFromAndWriteToBytes;


#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
pub struct DbTime {
    pub hour: u16,
    pub minute: u16,
    pub second: u16,
    pub padding: u16,
}
