use std::fs::File;
use std::path::PathBuf;

use clap::Parser;
use esedb::header::read_header;
use esedb::page::{read_data_for_tag, read_page_header, read_page_tags, read_root_page_header};


#[derive(Parser)]
struct Opts {
    pub db_path: PathBuf,
}


fn main() {
    let opts = Opts::parse();
    let mut file = File::open(&opts.db_path)
        .expect("failed to open database file");
    let header = read_header(&mut file)
        .expect("failed to read database header");
    println!("{:#?}", header);
    let shadow_header = read_header(&mut file)
        .expect("failed to read database shadow header");
    if header != shadow_header {
        println!("warning: shadow header mismatch");
    }

    // page 4 contains the catalog of objects
    let page_4_header = read_page_header(&mut file, &header, 4)
        .expect("failed to read page 4 header");
    println!("{:#?}", page_4_header);

    let page_4_tags = read_page_tags(&mut file, &header, &page_4_header)
        .expect("failed to read page 4 tags");
    println!("{:#?}", page_4_tags);

    for page_tag in &page_4_tags {
        let data = read_data_for_tag(&mut file, &header, &page_4_header, page_tag)
            .expect("failed to read data for tag");
        println!("{:?}", data);
    }
}
