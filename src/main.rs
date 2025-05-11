use std::{
    io::{stdout, Write},
    path::Path,
    process::exit,
};

use simpledb::{
    rdbc::{
        connection_adapter::ConnectionAdapter,
        driver_adapter::DriverAdapter,
        embedded::{
            embedded_connection::EmbeddedConnection, embedded_driver::EmbeddedDriver,
            embedded_metadata::EmbeddedMetadata, embedded_result_set::EmbeddedResultSet,
            embedded_statement::EmbeddedStatement,
        },
        result_set_adapter::ResultSetAdapter,
        result_set_metadata_adapter::ResultSetMetadataAdapter,
        sql_exception::SQLException,
        statement_adapter::StatementAdapter,
    },
    record::schema::field_type,
};

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
    println!("{}", dbpath);
    let dbpath = Path::new(&dbpath);
    let mut drvr = EmbeddedDriver::connect(dbpath);

    while let Ok(qry) = read_query() {
        exec(&mut drvr, &qry);
    }
}

fn read_query() -> Result<String, String> {
    print!("sabidb>");
    stdout().flush().expect("require input");

    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .map_err(|_| "could not read")?;
    Ok(input)
}

fn exec(conn: &mut EmbeddedConnection, qry: &String) {
    let words: Vec<&str> = qry.split_whitespace().collect();
    if words.is_empty() {
        return;
    }
    if &words[0].trim().to_ascii_lowercase() == "exit" {
        println!("bye");
        exit(0)
    }

    let mut stmt = conn.create_statement().expect("create statement");
    let cmd = words[0].trim().to_ascii_lowercase();
    if &cmd == "select" {
        exec_query(&mut stmt, qry);
    } else {
        exec_update(&mut stmt, qry).unwrap();
    }
}

fn exec_query<'a>(stmt: &'a mut EmbeddedStatement<'a>, sql: &String) {
    match stmt.execute_query(sql) {
        Ok(result) => {
            let cnt = print_result_set(result).unwrap();
            println!("Rows: {}", cnt)
        }
        Err(_) => println!("invalid query {}", sql),
    }
}

fn print_result_set(mut result: EmbeddedResultSet) -> Result<i32, SQLException> {
    let meta = result.get_metadata()?;

    for i in 0..meta.get_column_count()? {
        let name = meta.get_column_name(i).expect("get column name").unwrap();
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        print!("{:width$} ", name, width = w as usize);
    }
    println!();

    for i in 0..meta.get_column_count()? {
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        print!("{:-<width$}", "", width = w as usize + 1);
    }
    println!();
    let mut c = 0;
    while result.next().unwrap() {
        c += 1;
        print_record(&mut result, &meta)?;
    }

    result.close()?;

    Ok(c)
}

fn print_record(
    results: &mut EmbeddedResultSet,
    meta: &EmbeddedMetadata,
) -> Result<(), SQLException> {
    for i in 0..meta.get_column_count().unwrap() {
        let fldname = meta.get_column_name(i).expect("get column name").unwrap();
        let w = meta
            .get_column_display_size(i)
            .expect("get column display size");
        match meta.get_column_type(i).expect("get column type") {
            Some(type_i) => {
                if type_i == field_type::INTEGER {
                    print!("{:width$} ", results.get_int(fldname)?, width = w as usize);
                } else if type_i == field_type::VARCHAR {
                    print!(
                        "{:width$} ",
                        results.get_string(fldname)?,
                        width = w as usize
                    );
                } else {
                    panic!("unexpected field type");
                }
            }
            None => panic!("unreachable"),
        }
    }
    println!();

    Ok(())
}

fn exec_update<'a>(stmt: &'a mut EmbeddedStatement<'a>, sql: &String) -> Result<(), SQLException> {
    match stmt.execute_update(sql) {
        Ok(affected) => {
            println!("affected: {}", affected)
        }
        Err(_) => {
            println!("invalid command: {}", sql)
        }
    }

    Ok(())
}
