use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{
    buffer::buffer_manager::BufferManager,
    constants::LOG_FILE,
    file::file_manager::FileManager,
    index::planner::index_update_planner::IndexUpdatePlanner,
    log::log_manager::LogManager,
    metadata::matadata_manager::MetadataManager,
    opt::heuristic_query_planner::HeuristicQueryPlanner,
    plan::{
        basic_query_planner::BasicQueryPlanner, basic_update_planner::BasicUpdatePlanner,
        planner::Planner,
    },
    tx::transaction::Transaction,
};

const BLOCK_SISE: i32 = 400;
const BUFFER_SISE: i32 = 8;

pub struct SimpleDB {
    fm: Arc<Mutex<FileManager>>,
    lm: Arc<Mutex<LogManager>>,
    bm: Arc<Mutex<BufferManager>>,
    mdm: Option<Arc<Mutex<MetadataManager>>>,
    pub planner: Option<Planner>,
}

impl SimpleDB {
    pub fn new_with_sizes(dirname: &Path, blocksize: i32, buffsize: i32) -> Self {
        let fm = Arc::new(Mutex::new(FileManager::new_from_blocksize(
            &dirname, blocksize,
        )));
        let lm = Arc::new(Mutex::new(
            LogManager::new(fm.clone(), LOG_FILE.to_string()).unwrap(),
        ));
        let bm = Arc::new(Mutex::new(
            BufferManager::new(fm.clone(), lm.clone(), buffsize).unwrap(),
        ));
        Self {
            fm,
            lm,
            bm,
            mdm: None,
            planner: None,
        }
    }

    pub fn new(dirname: &Path) -> Self {
        let mut db = Self::new_with_sizes(dirname, BLOCK_SISE, BUFFER_SISE);
        let tx = db.new_tx();
        let is_new = db.fm.lock().unwrap().is_new();
        if is_new {
            println!("creating new database")
        } else {
            println!("recovering existing database")
        }

        let mdm = Arc::new(Mutex::new(
            MetadataManager::new(is_new, tx.clone()).unwrap(),
        ));
        let qp = BasicQueryPlanner::new(mdm.clone());
        let up = BasicUpdatePlanner::new(mdm.clone());

        let planner = Planner::new(Arc::new(Mutex::new(qp)), Arc::new(Mutex::new(up)));
        db.mdm = Some(mdm);
        db.planner = Some(planner);

        tx.lock().unwrap().commit().unwrap();

        db
    }

    pub fn new_with_refined_planners(dirname: &Path) -> Self {
        let mut db = Self::new_with_sizes(dirname, BLOCK_SISE, BUFFER_SISE);
        print_logo();

        let tx = db.new_tx();
        let is_new = db.fm.lock().unwrap().is_new();
        if is_new {
            println!("creating new database")
        } else {
            println!("recovering existing database")
        }

        let mdm = Arc::new(Mutex::new(
            MetadataManager::new(is_new, tx.clone()).unwrap(),
        ));

        let qp = HeuristicQueryPlanner::new(mdm.clone());
        let up = IndexUpdatePlanner::new(mdm.clone());

        let planner = Planner::new(Arc::new(Mutex::new(qp)), Arc::new(Mutex::new(up)));
        db.mdm = Some(mdm);
        db.planner = Some(planner);

        tx.lock().unwrap().commit().unwrap();

        db
    }

    pub fn metadata_manager(&self) -> Arc<Mutex<MetadataManager>> {
        self.mdm.clone().unwrap()
    }

    pub fn file_manager(&self) -> Arc<Mutex<FileManager>> {
        self.fm.clone()
    }

    pub fn log_mgr(&self) -> Arc<Mutex<LogManager>> {
        self.lm.clone()
    }

    pub fn buffer_manager(&self) -> Arc<Mutex<BufferManager>> {
        self.bm.clone()
    }

    pub fn new_tx(&self) -> Arc<Mutex<Transaction>> {
        Arc::new(Mutex::new(
            Transaction::new_from_managers(self.fm.clone(), self.lm.clone(), self.bm.clone())
                .unwrap(),
        ))
    }
}

fn print_logo() {
    println!("\x1b[38;5;208m");

    println!(
        r#"
 ___    __    ____  ____  ____  ____ 
/ __)  /__\  (  _ \(_  _)(  _ \(  _ \
\__ \ /(__)\  ) _ < _)(_  )(_) )) _ <
(___/(__)(__)(____/(____)(____/(____/"#
    );

    println!("\x1b[0m");
}

#[cfg(test)]
mod integration_tests {

    use tempfile::TempDir;

    use crate::{server::simple_db::SimpleDB, testlib::helper::create_student_data};

    #[test]
    fn test_planner1() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SimpleDB::new_with_refined_planners(temp_dir.path());
        create_student_data(&mut db);

        let tx = db.new_tx();
        let mut planner = db.planner.unwrap();
        let cmd = "select sid, sname, did, dname, cid, title from students, depts, courses";
        let s = planner
            .create_query_planner(&cmd.to_string(), tx)
            .unwrap()
            .lock()
            .unwrap()
            .open()
            .unwrap();
        while s.lock().unwrap().next().unwrap() {
            let locked_s = s.lock().unwrap();

            let sid = locked_s.get_int(&"sid".to_string()).unwrap();
            let sname = locked_s.get_string(&"sname".to_string()).unwrap();
            let did = locked_s.get_int(&"did".to_string()).unwrap();
            let dname = locked_s.get_string(&"dname".to_string()).unwrap();
            let cid = locked_s.get_int(&"cid".to_string()).unwrap();
            let title = locked_s.get_string(&"title".to_string()).unwrap();
            println!("{} {} {} {} {} {}", sid, sname, did, dname, cid, title)
        }
    }

    #[test]
    fn test_planner2() {
        let temp_dir = TempDir::new().unwrap();
        // let db = SimpleDB::new(temp_dir.path());
        let db = SimpleDB::new_with_refined_planners(temp_dir.path());
        let tx = db.new_tx();
        let mut planner = db.planner.unwrap();

        let cmd = "create table T(a int, b varchar(9), c int, d varchar(9), e int, f varchar(9), g int, h varchar(9))";
        planner.execute_update(cmd, tx.clone()).unwrap();

        let n = 200;
        for i in 0..n {
            let a = i;
            let b = format!("bbb{}", a);
            let c = i;
            let d = format!("ddd{}", c);
            let e = i;
            let f = format!("fff{}", e);
            let g = i;
            let h = format!("hhh{}", g);
            let cmd = format!(
                "insert into T(a,b,c,d,e,f,g,h) values ({}, '{}',{}, '{}',{}, '{}',{}, '{}')",
                a, b, c, d, e, f, g, h
            );
            planner.execute_update(&cmd, tx.clone()).unwrap();
        }

        let cmd = "create table TT(i int, j varchar(8), k int, l varchar(9), m int, n varchar(9), o int, p varchar(9))";

        planner.execute_update(cmd, tx.clone()).unwrap();
        let n = 200;
        for i in 0..n {
            let ii = i;
            let j = format!("lll{}", ii);
            let k = i;
            let l = format!("lll{}", k);
            let m = i;
            let nn = format!("nnn{}", m);
            let o = i;
            let p = format!("ppp{}", o);
            let cmd = format!(
                "insert into TT(i,j,k,l,m,n,o,p) values ({}, '{}',{}, '{}',{}, '{}',{}, '{}')",
                ii, j, k, l, m, nn, o, p
            );

            planner.execute_update(&cmd, tx.clone()).unwrap();
        }

        let qry = "select a, b, i, j from T,TT where a=i";
        let p = planner
            .create_query_planner(&qry.to_string(), tx.clone())
            .unwrap();
        let s = p.lock().unwrap().open().unwrap();
        let mut locked_s = s.lock().unwrap();
        while locked_s.next().unwrap() {
            println!(
                "{} {}",
                locked_s.get_string(&"b".to_string()).unwrap(),
                locked_s.get_string(&"j".to_string()).unwrap()
            );
            assert_eq!(
                locked_s.get_int(&"a".to_string()),
                locked_s.get_int(&"i".to_string())
            )
        }
        locked_s.close().unwrap();
        tx.lock().unwrap().commit().unwrap();
    }
}
