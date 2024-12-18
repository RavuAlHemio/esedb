use std::fs::File;
use std::path::{Path, PathBuf};

use clap::Parser;
use esedb::header::read_header;
use esedb::page::CATALOG_PAGE_NUMBER;
use esedb::table::{collect_tables, read_table_from_pages};


#[derive(Parser)]
enum Opts {
    Tables(TablesOpts),
    DumpTable(DumpTableOpts),
}
impl Opts {
    pub fn db_path(&self) -> &Path {
        match self {
            Self::Tables(to) => to.db_path.as_path(),
            Self::DumpTable(dto) => dto.db_path.as_path(),
        }
    }
}

#[derive(Parser)]
struct TablesOpts {
    pub db_path: PathBuf,
}

#[derive(Parser)]
struct DumpTableOpts {
    pub db_path: PathBuf,
    pub table: String,
}


fn main() {
    // set up logging/tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stdout)
        .pretty()
        .init();

    let opts = Opts::parse();
    let mut file = File::open(opts.db_path())
        .expect("failed to open database file");
    let header = read_header(&mut file)
        .expect("failed to read database header");
    let shadow_header = read_header(&mut file)
        .expect("failed to read database shadow header");
    if header != shadow_header {
        println!("warning: shadow header mismatch");
    }

    // read the catalog of objects
    let mut naive_rows = Vec::new();
    read_table_from_pages(&mut file, &header, CATALOG_PAGE_NUMBER, &*esedb::table::METADATA_COLUMN_DEFS, &mut naive_rows)
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

    match opts {
        Opts::Tables(_tables_opts) => {
            for table in &tables {
                println!("table {:?} ({})", table.header.name, table.header.table_object_id);
                println!("  flags {:?}", table.header.flags);
                for column in &table.columns {
                    println!("  column {:?} ({})", column.name, column.column_id);
                    println!("    flags {:?}", column.flags);
                    println!("    type {:?}", column.column_type);
                    println!("    length {}", column.length);
                    println!("    codepage {}", column.codepage);
                }
                for index in &table.indexes {
                    println!("  index {:?} ({})", index.name, index.index_id);
                    println!("    flags {:?}", index.flags);
                }
            }
        },
        Opts::DumpTable(dump_table_opts) => {
            // find table
            let table = tables.iter()
                .find(|t| t.header.name == dump_table_opts.table)
                .expect("requested table not found");

            let mut rows = Vec::new();
            read_table_from_pages(&mut file, &header, table.header.fdp_page_number.try_into().unwrap(), &table.columns, &mut rows)
                .expect("failed to read data rows");
            for row in &rows {
                println!("---");
                for column in &table.columns {
                    let Some(value) = row.get(&column.column_id) else { continue };
                    println!("{}={:?}", column.name, value);
                }
            }
        },
    }
}
