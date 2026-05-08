<div align="center">

# sabidb

```text
 ___    __    ____  ____  ____  ____ 
/ __)  /__\  (  _ \(_  _)(  _ \(  _ \
\__ \ /(__)\  ) _ < _)(_  )(_) )) _ <
(___/(__)(__)(____/(____)(____/(____/
```

**A Rust implementation of the SimpleDB database system.**

[![CI](https://github.com/awayatakuma/sabidb/actions/workflows/ci.yml/badge.svg)](https://github.com/awayatakuma/sabidb/actions/workflows/ci.yml)
![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)

Based on [Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7) by Edward Sciore.

</div>

---

## ✨ Features

- **Robust SQL Support**: Execute DDL (CREATE TABLE/VIEW/INDEX) and DML (SELECT/INSERT/UPDATE/DELETE).
- **ACID Transactions**: Full support for transactions with recovery from logs and checkpointing.
- **Efficient Indexing**: Includes B-Tree and Hash index implementations for fast data retrieval.
- **Advanced Query Planning**: Features both Basic and Heuristic (optimized) query planners.
- **Embedded Database**: Lightweight and easy to use as an embedded library (similar to SQLite).
- **Concurrency**: Multi-threaded access with proper latching and concurrency management.

## 🎮 Demo

![sabidb_basic](./sabidb_demo.gif 'sabidb_demo')

## 🚀 Quick Start

### Requirements
- [Rust toolchain](https://www.rust-lang.org/tools/install) (stable)

### Running Tests
Verify the installation and implementation:
```bash
cargo test
```

### Run Embedded DB
Start the embedded interactive shell:
```bash
cargo run --bin embedded
```

Or connect to a specific database:
```bash
cargo run --bin embedded -- -d studentdb
```

### 🛠️ Maintenance: Rebuilding Samples
If the schema changes or you want to reset the built-in sample database (`studentdb`), run:
```bash
cargo run --bin build-samples
```
This tool regenerates all tables, indexes, and sample data (including the `students` table with its new `boolean` fields).

#### Sample Queries
Once in the `sabidb>` shell, you can try:
```sql
select sid, sname, majorid, gradyear from students
```

## 📖 SQL Examples (Verified)

These scenarios are automatically verified by `cargo test` against both **Basic** and **Heuristic** planners.

### 1. Data Definition (DDL)
```sql
-- Create tables, indexes, and views
create table students(sid int, sname varchar(9), majorid int, gradyear int, is_active boolean)
create table depts(did int, dname varchar(8))
create index majorid_idx on students(majorid)
create view cs_students as select sid, sname from students where majorid = 10
```

### 2. Data Manipulation (DML)
```sql
-- Insert, Update, and Delete
insert into students(sid, sname, majorid, gradyear, is_active) values (1, 'joe', 10, 2021, true)
update students set is_active = false where sid = 1
delete from students where sid = 5
```

### 3. Querying
```sql
-- Basic selection
select sid, sname, is_active from students

-- Joins, View expansion, and Multiple conditions
select sname, dname from students, depts where majorid = did and is_active = true
select sid, sname from cs_students
```

---

## 🗺️ Roadmap

This roadmap is based on the [SamehadaDB](https://github.com/ryogrid/SamehadaDB) roadmap. Special thanks to [ryogrid](https://github.com/ryogrid) for the inspiration! 🙏

### SQL Features
- [x] Predicates on Seq Scan
- [x] Multiple Item on Predicate: AND (OR is not supported yet)
- [ ] Predicates: `<`, `>`, `<=`, `>=`
- [ ] Null
- [ ] Predicates: NOT
- [x] Delete Tuple
- [x] Update Tuple
- [ ] LIMIT / OFFSET
- [x] Varchar
- [x] Boolean
- [ ] AS clause
- [ ] Nested Query
- [x] Predicates: IN
- [ ] DISTINCT
- [ ] UNION clause

### Core Engine
- [x] Persistent Catalog
- [ ] Updating of Table Schema 
- [x] Latches (Thread safety)
- [x] Transactions (ACID)
- [x] Rollback When Abort Occurs
- [x] Logging & Recovery from Logs
- [x] Checkpointing (Simple)
  - [ ] Fuzzy Checkpointing (ARIES)
- [x] Deallocate and Reuse Page
- [x] Materialization Support

### Indexing
- [x] Hash Index (Thread-safe, equals operator only)
- [x] B-tree Index
- [ ] SkipList Index
- [ ] Logging And Recovery Of Index Data

### Join Algorithms
- [x] INNER JOIN (Hash Join, Index Join, Nested Loop Join)
- [ ] OUTER JOIN
- [x] CROSS JOIN

### Optimization & Planning
- [x] Statistics Data for Optimizer
- [x] Heuristic Query Planner (supports Index Join/Select)
- [ ] Query Optimization (Selinger)
- [ ] TRANSACTION Statement on SQL (multi-statement support)

### Connectivity
- [x] Frontend Impl as Embedded DB Library
- [ ] DB Connector (Driver) / Network Interface (MySQL/PostgreSQL Compatible)
- [ ] REST API
- [ ] ORM Support (e.g., Python/Django)

---

## 💡 Tips: Switching Planners

You can choose between the simple default planner or the optimized heuristic planner in your code:

```rust
// Use default Basic planner
let db = SimpleDB::new(dbpath);

// OR use optimized Heuristic planner
let db = SimpleDB::new_with_refined_planners(dbpath);
```
