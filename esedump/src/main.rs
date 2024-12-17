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

    let page_1_header = read_page_header(&mut file, &header, 1)
        .expect("failed to read page 1 header");
    println!("{:#?}", page_1_header);

    let page_1_tags = read_page_tags(&mut file, &header, &page_1_header)
        .expect("failed to read page 1 tags");
    println!("{:#?}", page_1_tags);

    let root_page_header_data = read_data_for_tag(&mut file, &header, &page_1_header, &page_1_tags[0])
        .expect("failed to read root page header data");
    let root_page_header = read_root_page_header(&root_page_header_data)
        .expect("failed to parse root page header");
    println!("{:#?}", root_page_header);

    let space_page_number = root_page_header.space_tree_page_number();
    let space_page_header = read_page_header(&mut file, &header, space_page_number.into())
        .expect("failed to read space page header");
    println!("{:#?}", space_page_header);

    let space_page_tags = read_page_tags(&mut file, &header, &space_page_header)
        .expect("failed to read space page tags");
    println!("{:#?}", space_page_tags);

    for space_page_tag in &space_page_tags {
        let space_tag_data = read_data_for_tag(&mut file, &header, &space_page_header, space_page_tag)
            .expect("failed to read data");
        println!("{:#?}", space_tag_data);
    }
}
