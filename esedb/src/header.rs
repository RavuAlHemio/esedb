use esedb_macros::ReadFromAndWriteToBytes;
use from_to_repr::from_to_other;


#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
pub struct Header {
    pub checksum: u32,
    pub signature: u32,
    pub version: u32,
    pub file_type: FileType, // u32
    pub database_time: DbTime, // u64
    pub db_signature: DbSignature, // 28 bytes
    pub state: DbState, // u32
    pub consistent_position: LogPosition, // u64
    pub consistent_timestamp: LogTime, // u64
    pub attach_timestamp: LogTime, // u64
    pub attach_position: LogPosition, // u64
    pub detach_timestamp: LogTime, // u64
    pub detach_position: LogPosition, // u64
    pub dbid: u32,
    pub log_signature: DbSignature, // 28 bytes
    pub previous_full_backup: BackupInfo, // 24 bytes
    pub previous_incremental_backup: BackupInfo, // 24 bytes
    pub current_full_backup: BackupInfo, // 24 bytes
    pub shadowing_disabled: u32,
    pub last_oid: u32,
    pub last_index_update_version: NtVersion, // 16 bytes
    pub format_revision: u32,
    pub page_size: u32,
    pub repair_count: u32,
    pub repair_timestamp: LogTime, // u64
    pub unknown2: DbSignature, // 28 bytes
    pub scrub_time: DbTime, // u64
    pub scrub_timestamp: LogTime, // u64
    pub required_log: u64,
    pub upgrade_exchange55: u32,
    pub upgrade_free_pages: u32,
    pub upgrade_space_map_pages: u32,
    pub current_shadow_copy_backup: BackupInfo, // 24 bytes
    pub creation_version: u32,
    pub creation_revision: u32,
    pub unknown3: [u8; 16],
    pub old_repair_count: u32,
    pub ecc_fix_success: ErrorStats, // 16 bytes
    pub ecc_fix_error: ErrorStats, // 16 bytes
    pub bad_checksum_error: ErrorStats, // 16 bytes
    pub committed_log: u32,
    pub previous_shadow_copy_backup: BackupInfo, // 24 bytes
    pub previous_differential_backup: BackupInfo, // 24 bytes
    pub unknown4: [u8; 40],
    pub nls_major_version: u32,
    pub nls_minor_version: u32,
    pub unknown5: [u8; 148],
    pub unknown_flags: u32,
}


#[derive(Clone, Copy, Debug, ReadFromAndWriteToBytes)]
#[from_to_other(base_type = u32, derive_compare = "as_int")]
pub enum FileType {
    Database = 0,
    StreamingFile = 1,
    Other(u32),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
pub struct DbTime {
    pub hour: u16,
    pub minute: u16,
    pub second: u16,
    pub padding: u16,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
pub struct LogTime {
    pub second: u8,
    pub minute: u8,
    pub hour: u8,
    pub day: u8,
    pub month: u8,
    pub year: u8,
    pub padding: u8,
    pub backup_type: BackupType,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
pub struct DbSignature {
    pub random_number: u32,
    pub creation_timestamp: LogTime,
    pub computer_name: [u8; 16],
}

#[derive(Clone, Copy, Debug, ReadFromAndWriteToBytes)]
#[from_to_other(base_type = u8, derive_compare = "as_int")]
pub enum BackupType {
    Streaming = 0,
    Snapshot = 1,
    Other(u8),
}

#[derive(Clone, Copy, Debug, ReadFromAndWriteToBytes)]
#[from_to_other(base_type = u32, derive_compare = "as_int")]
pub enum DbState {
    JustCreated = 1,
    DirtyShutdown = 2,
    CleanShutdown = 3,
    BeingConverted = 4,
    ForceDetach = 5,
    Other(u32),
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
pub struct LogPosition {
    pub block: u16,
    pub sector: u16,
    pub generation: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
pub struct BackupInfo {
    pub position: LogPosition, // u64
    pub timestamp: LogTime, // u64
    pub generation_lower: u32,
    pub generation_upper: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
pub struct NtVersion {
    pub major: u32,
    pub minor: u32,
    pub build: u32,
    pub service_pack: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, ReadFromAndWriteToBytes)]
pub struct ErrorStats {
    pub count: u32,
    pub last_timestamp: LogTime,
    pub old_count: u32,
}
