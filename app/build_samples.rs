use std::path::Path;
use sabidb::server::simple_db::SimpleDB;
use sabidb::testlib::helper::create_student_data;

fn main() {
    let dbpath = Path::new("sabidb/studentdb");
    
    println!("Building sample database at: {:?}", dbpath);

    if dbpath.exists() {
        println!("Removing existing sample database...");
        std::fs::remove_dir_all(dbpath).expect("failed to remove old db");
    }
    
    let mut db = SimpleDB::new(dbpath);
    create_student_data(&mut db);
    
    println!("Successfully built 'studentdb' with latest schema and data.");
}
