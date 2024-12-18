mod schema;


use std::fs::File;
use std::path::PathBuf;

use clap::Parser;
use esedb::header::read_header;
use esedb::page::CATALOG_PAGE_NUMBER;
use esedb::table::{collect_tables, read_table_from_pages};

use crate::schema::{collect_schema_attributes, find_schema_root};


#[derive(Parser)]
struct Opts {
    pub db_path: PathBuf,
}


fn main() {
    // set up logging/tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .pretty()
        .init();

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

    // read the catalog of objects
    let naive_rows = read_table_from_pages(&mut file, &header, CATALOG_PAGE_NUMBER, &*esedb::table::METADATA_COLUMN_DEFS, None)
        .expect("failed to read metadata table from pages");
    let naive_tables = collect_tables(&naive_rows, &*esedb::table::METADATA_COLUMN_DEFS)
        .expect("failed to collect tables");

    // find the MSysObjects table
    let mso = naive_tables.iter()
        .find(|t| t.header.name == "MSysObjects")
        .expect("MSysObjects table not found");

    // re-read the metadata given this definition
    let meta_rows = read_table_from_pages(&mut file, &header, mso.header.fdp_page_number.try_into().unwrap(), &mso.columns, mso.long_value_page_number())
        .expect("failed to read metadata table from pages");
    let tables = collect_tables(&meta_rows, &mso.columns)
        .expect("failed to collect tables");

    // find datatable and read it
    let d8a = tables.iter()
        .find(|t| t.header.name == "datatable")
        .expect("datatable not found");
    let d8a_rows = read_table_from_pages(&mut file, &header, d8a.header.fdp_page_number.try_into().unwrap(), &d8a.columns, d8a.long_value_page_number())
        .expect("failed to read data rows");

    let schema_root = find_schema_root(d8a, &d8a_rows);
    //let id_to_class = collect_schema_classes(d8a, &d8a_rows, schema_root);
    let name_to_attribute = collect_schema_attributes(d8a, &d8a_rows, schema_root);

    // run through the datatable
    for d8a_row in &d8a_rows {
        println!("---");
        for (col_id, value) in d8a_row {
            let column = d8a.columns.iter().find(|c| c.column_id == *col_id).unwrap();
            if let Some(attribute) = name_to_attribute.get(&column.name) {
                print!("{}: ", attribute.ldap_name);
            } else {
                print!("{}: ", column.name);
            }
            println!("{:?}", value);
        }
    }
}
