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
    tx::{concurrency::lock_table::LockTable, transaction::Transaction},
};

const BLOCK_SISE: i32 = 400;
const BUFFER_SISE: i32 = 8;

pub struct SimpleDB {
    fm: Arc<FileManager>,
    lm: Arc<Mutex<LogManager>>,
    bm: Arc<Mutex<BufferManager>>,
    lt: Arc<Mutex<LockTable>>,
    mdm: Option<Arc<Mutex<MetadataManager>>>,
    pub planner: Option<Planner>,
}

impl SimpleDB {
    pub fn new_with_sizes(dirname: &Path, blocksize: i32, buffsize: i32) -> Self {
        let fm = Arc::new(FileManager::new_from_blocksize(dirname, blocksize));
        let lm = Arc::new(Mutex::new(
            LogManager::new(fm.clone(), LOG_FILE.to_string()).unwrap(),
        ));
        let bm = Arc::new(Mutex::new(
            BufferManager::new(fm.clone(), lm.clone(), buffsize).unwrap(),
        ));
        let lt = Arc::new(Mutex::new(LockTable::new()));
        Self {
            fm,
            lm,
            bm,
            lt,
            mdm: None,
            planner: None,
        }
    }

    pub fn new(dirname: &Path) -> Self {
        let mut db = Self::new_with_sizes(dirname, BLOCK_SISE, BUFFER_SISE);
        print_logo();
        let tx = db.new_tx();
        let is_new = db.fm.is_new();
        if is_new {
            println!("creating new database")
        } else {
            println!("recovering existing database");
            tx.lock().unwrap().recover().unwrap();
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
        let is_new = db.fm.is_new();
        if is_new {
            println!("creating new database")
        } else {
            println!("recovering existing database");
            tx.lock().unwrap().recover().unwrap();
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

    pub fn file_manager(&self) -> Arc<FileManager> {
        self.fm.clone()
    }

    pub fn log_mgr(&self) -> Arc<Mutex<LogManager>> {
        self.lm.clone()
    }

    pub fn buffer_manager(&self) -> Arc<Mutex<BufferManager>> {
        self.bm.clone()
    }

    pub fn lock_table(&self) -> Arc<Mutex<LockTable>> {
        self.lt.clone()
    }

    pub fn new_tx(&self) -> Arc<Mutex<Transaction>> {
        Arc::new(Mutex::new(
            Transaction::new_from_managers(
                self.fm.clone(),
                self.lm.clone(),
                self.bm.clone(),
                self.lt.clone(),
            )
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

    use std::path::Path;

    use tempfile::TempDir;

    use crate::{
        file::{block_id::BlockId, page::Page},
        server::simple_db::SimpleDB,
        testlib::helper::create_student_data,
    };

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

    #[test]
    fn basic_constructor_recovers_uncommitted_flushed_page() {
        assert_constructor_recovers(SimpleDB::new);
    }

    #[test]
    fn refined_constructor_recovers_uncommitted_flushed_page() {
        assert_constructor_recovers(SimpleDB::new_with_refined_planners);
    }

    #[test]
    fn recovery_undoes_every_uncommitted_tx() {
        const INITIAL_VALUE: i32 = 7;
        const UNCOMMITTED_VALUE: i32 = 99;
        const COMMITTED_VALUE: i32 = 42;

        let temp_dir = TempDir::new().unwrap();
        let filename = "multi_tx_recovery_test.tbl".to_string();
        let blks: Vec<BlockId>;

        {
            let db = SimpleDB::new(temp_dir.path());

            // Baseline: three committed blocks holding INITIAL_VALUE.
            let setup_tx = db.new_tx();
            blks = (0..3)
                .map(|_| {
                    let blk = setup_tx.lock().unwrap().append(filename.clone()).unwrap();
                    setup_tx.lock().unwrap().pin(&blk).unwrap();
                    setup_tx
                        .lock()
                        .unwrap()
                        .set_int(&blk, 0, INITIAL_VALUE, true)
                        .unwrap();
                    blk
                })
                .collect();
            setup_tx.lock().unwrap().commit().unwrap();

            // Two transactions stay uncommitted and one commits. Each writes to
            // its own block so they never contend for the same lock.
            let uncommitted1 = db.new_tx();
            let uncommitted2 = db.new_tx();
            let committed = db.new_tx();

            uncommitted1.lock().unwrap().pin(&blks[0]).unwrap();
            uncommitted1
                .lock()
                .unwrap()
                .set_int(&blks[0], 0, UNCOMMITTED_VALUE, true)
                .unwrap();
            uncommitted2.lock().unwrap().pin(&blks[1]).unwrap();
            uncommitted2
                .lock()
                .unwrap()
                .set_int(&blks[1], 0, UNCOMMITTED_VALUE, true)
                .unwrap();
            committed.lock().unwrap().pin(&blks[2]).unwrap();
            committed
                .lock()
                .unwrap()
                .set_int(&blks[2], 0, COMMITTED_VALUE, true)
                .unwrap();

            // The COMMIT record written here is what puts this transaction into
            // do_recover's finished list, so its write survives while the two
            // interleaved uncommitted ones are undone.
            committed.lock().unwrap().commit().unwrap();

            // Simulate a steal before a crash: both uncommitted pages reach disk
            // with no COMMIT or ROLLBACK record behind them.
            for tx in [&uncommitted1, &uncommitted2] {
                let txnum = tx.lock().unwrap().tx_num();
                db.buffer_manager()
                    .lock()
                    .unwrap()
                    .flush_all(txnum)
                    .unwrap();
            }
            uncommitted1.lock().unwrap().unpin(&blks[0]).unwrap();
            uncommitted2.lock().unwrap().unpin(&blks[1]).unwrap();
        }

        let recovered_db = SimpleDB::new(temp_dir.path());
        let mut page = Page::new_from_blocksize(400);
        recovered_db
            .file_manager()
            .read(&blks[0], &mut page)
            .unwrap();
        assert_eq!(page.get_int(0).unwrap(), INITIAL_VALUE);
        recovered_db
            .file_manager()
            .read(&blks[1], &mut page)
            .unwrap();
        assert_eq!(page.get_int(0).unwrap(), INITIAL_VALUE);
        recovered_db
            .file_manager()
            .read(&blks[2], &mut page)
            .unwrap();
        assert_eq!(page.get_int(0).unwrap(), COMMITTED_VALUE);
    }

    #[test]
    fn commit_forces_pages_to_disk_so_recovery_needs_no_redo() {
        const COMMITTED_VALUE: i32 = 123;

        let temp_dir = TempDir::new().unwrap();
        let filename = "force_policy_test.tbl".to_string();
        let blk: BlockId;

        {
            let db = SimpleDB::new(temp_dir.path());
            let tx = db.new_tx();
            blk = tx.lock().unwrap().append(filename).unwrap();
            tx.lock().unwrap().pin(&blk).unwrap();
            tx.lock()
                .unwrap()
                .set_int(&blk, 0, COMMITTED_VALUE, true)
                .unwrap();
            tx.lock().unwrap().commit().unwrap();

            // commit() flushes the transaction's buffers before writing its
            // COMMIT record (force policy), so the value is already on disk
            // without any explicit flush here. That is precisely why
            // do_recover only ever undoes and never needs a redo pass.
            let mut page = Page::new_from_blocksize(400);
            db.file_manager().read(&blk, &mut page).unwrap();
            assert_eq!(page.get_int(0).unwrap(), COMMITTED_VALUE);
        }

        // Recovery on restart must leave the committed value untouched.
        let recovered_db = SimpleDB::new(temp_dir.path());
        let mut page = Page::new_from_blocksize(400);
        recovered_db.file_manager().read(&blk, &mut page).unwrap();
        assert_eq!(page.get_int(0).unwrap(), COMMITTED_VALUE);
    }

    fn assert_constructor_recovers(open: fn(&Path) -> SimpleDB) {
        const INITIAL_VALUE: i32 = 7;
        const UNCOMMITTED_VALUE: i32 = 99;

        let temp_dir = TempDir::new().unwrap();
        let filename = "recovery_test.tbl".to_string();
        let blk: BlockId;

        {
            let db = open(temp_dir.path());

            let initial_tx = db.new_tx();
            blk = initial_tx.lock().unwrap().append(filename.clone()).unwrap();
            initial_tx.lock().unwrap().pin(&blk).unwrap();
            initial_tx
                .lock()
                .unwrap()
                .set_int(&blk, 0, INITIAL_VALUE, true)
                .unwrap();
            initial_tx.lock().unwrap().commit().unwrap();

            let uncommitted_tx = db.new_tx();
            uncommitted_tx.lock().unwrap().pin(&blk).unwrap();
            uncommitted_tx
                .lock()
                .unwrap()
                .set_int(&blk, 0, UNCOMMITTED_VALUE, true)
                .unwrap();

            // Simulate a steal before a crash: WAL and the uncommitted data page
            // reach disk, but no COMMIT or ROLLBACK record is written.
            let txnum = uncommitted_tx.lock().unwrap().tx_num();
            db.buffer_manager()
                .lock()
                .unwrap()
                .flush_all(txnum)
                .unwrap();
            uncommitted_tx.lock().unwrap().unpin(&blk).unwrap();
        }

        let recovered_db = open(temp_dir.path());
        let mut page = Page::new_from_blocksize(400);
        recovered_db.file_manager().read(&blk, &mut page).unwrap();
        assert_eq!(page.get_int(0).unwrap(), INITIAL_VALUE);
    }
}
