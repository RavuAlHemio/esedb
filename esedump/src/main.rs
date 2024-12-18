use std::fs::File;
use std::path::PathBuf;

use clap::Parser;
use esedb::header::read_header;
use esedb::table::{collect_tables, read_table_from_pages};


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
    let shadow_header = read_header(&mut file)
        .expect("failed to read database shadow header");
    if header != shadow_header {
        println!("warning: shadow header mismatch");
    }

    // page 4 contains the catalog of objects
    let mut naive_rows = Vec::new();
    read_table_from_pages(&mut file, &header, 4, &*esedb::table::METADATA_COLUMN_DEFS, &mut naive_rows)
        .expect("failed to read metadata table from pages");
    let naive_tables = collect_tables(&naive_rows, &*esedb::table::METADATA_COLUMN_DEFS)
        .expect("failed to collect tables");

    // find the MSysObjects table
    let mso = naive_tables.iter()
        .find(|t| t.header.name == "MSysObjects")
        .expect("MSysObjects table not found");

    // re-read the metadata given this definition
    let mut meta_rows = Vec::new();
    read_table_from_pages(&mut file, &header, mso.header.fdp_page_number.try_into().unwrap(), &mso.columns, &mut meta_rows)
        .expect("failed to read metadata table from pages");
    let tables = collect_tables(&meta_rows, &mso.columns)
        .expect("failed to collect tables");

    // find datatable
    let d8a = tables.iter()
        .find(|t| t.header.name == "datatable")
        .expect("datatable not found");

    // read it
    let mut d8a_rows = Vec::new();
    read_table_from_pages(&mut file, &header, d8a.header.fdp_page_number.try_into().unwrap(), &d8a.columns, &mut d8a_rows)
        .expect("failed to read data rows");

    for row in &d8a_rows {
        println!("---");
        for column in &d8a.columns {
            let Some(value) = row.get(&column.column_id) else { continue };
            println!("{}={:?}", column.name, value);
        }
    }
}
