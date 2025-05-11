use std::path::Path;

pub mod utils;

use sabidb::rdbc::{driver_adapter::DriverAdapter, embedded::embedded_driver::EmbeddedDriver};

use clap::Parser;

#[derive(Debug, Parser)]
struct Args {
    #[arg(help = "dbname", short)]
    dbname: Option<String>,
}

fn main() {
    let args = Args::parse();
    let dbpath = format!(
        "sabidb/{}",
        if let Some(dbname) = args.dbname {
            dbname
        } else {
            "studentdb".to_string()
        }
    );
    let dbpath = Path::new(&dbpath);
    let mut drvr = EmbeddedDriver::connect(dbpath);

    while let Ok(qry) = utils::read_query() {
        utils::exec(&mut drvr, &qry);
    }
}
