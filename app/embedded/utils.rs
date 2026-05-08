use std::{
    io::{stdout, Write},
    process::exit,
};

use sabidb::{
    rdbc::{
        connection_adapter::ConnectionAdapter,
        embedded::{
            embedded_connection::EmbeddedConnection, embedded_metadata::EmbeddedMetadata,
            embedded_result_set::EmbeddedResultSet, embedded_statement::EmbeddedStatement,
        },
        result_set_adapter::ResultSetAdapter,
        result_set_metadata_adapter::ResultSetMetadataAdapter,
        sql_exception::SQLException,
        statement_adapter::StatementAdapter,
    },
    record::schema::field_type,
};

pub fn read_query() -> Result<String, String> {
    print!("sabidb>");
    stdout().flush().expect("require input");

    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .map_err(|_| "could not read")?;
    Ok(input)
}

pub fn exec(conn: &mut EmbeddedConnection, qry: &String) {
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
        Ok(result) => match print_result_set(result) {
            Ok(cnt) => println!("Rows: {}", cnt),
            Err(e) => println!("Error printing results: {}", e),
        },
        Err(e) => println!("Invalid query: {}. Error: {}", sql, e),
    }
}

fn print_result_set(mut result: EmbeddedResultSet) -> Result<i32, SQLException> {
    let meta = result.get_metadata()?;

    for i in 0..meta.get_column_count()? {
        let name = meta.get_column_name(i)?.ok_or_else(|| SQLException::new("column name is none".to_string()))?;
        let w = meta.get_column_display_size(i)?;
        print!("{:width$} ", name, width = w as usize);
    }
    println!();

    for i in 0..meta.get_column_count()? {
        let w = meta.get_column_display_size(i)?;
        print!("{:-<width$}", "", width = w as usize + 1);
    }
    println!();

    result.before_first()?;
    let mut c = 0;
    while result.next()? {
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
    for i in 0..meta.get_column_count()? {
        let fldname = meta.get_column_name(i)?.ok_or_else(|| SQLException::new("field name is none".to_string()))?;
        let w = meta.get_column_display_size(i)?;
        match meta.get_column_type(i)? {
            Some(type_i) => {
                if type_i == field_type::INTEGER {
                    print!("{:width$} ", results.get_int(fldname)?, width = w as usize);
                } else if type_i == field_type::VARCHAR {
                    print!(
                        "{:width$} ",
                        results.get_string(fldname)?,
                        width = w as usize
                    );
                } else if type_i == field_type::BOOLEAN {
                    print!(
                        "{:width$} ",
                        results.get_bool(fldname)?,
                        width = w as usize
                    );
                } else {
                    return Err(SQLException::new(format!("unexpected field type {}", type_i)));
                }
            }
            None => return Err(SQLException::new("field type is none".to_string())),
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
