use std::fs::File;
use std::path::PathBuf;

use clap::Parser;
use esedb::header::Header;


#[derive(Parser)]
struct Opts {
    pub db_path: PathBuf,
}


fn main() {
    let opts = Opts::parse();
    let file = File::open(&opts.db_path)
        .expect("failed to open database file");
    let mut reader = esedb::byte_io::LittleEndianRead::new(file);
    let header: esedb::header::Header = esedb::byte_io::ReadFromBytes::read_from_bytes(&mut reader)
        .expect("failed to read header");
    println!("{:#?}", header);
}
