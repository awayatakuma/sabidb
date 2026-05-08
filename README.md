# sabidb

```
 ___    __    ____  ____  ____  ____ 
/ __)  /__\  (  _ \(_  _)(  _ \(  _ \
\__ \ /(__)\  ) _ < _)(_  )(_) )) _ <
(___/(__)(__)(____/(____)(____/(____/
```

Rust implementation of SimpleDB from [Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7 "Database Design and Implementation")

This application is only for local runtime.

![sabidb_basic](./sabidb_demo.gif 'sabidb_demo')

## Roadmap

This roadmap is referred to  [SamehadaDB/README.md at master · ryogrid/SamehadaDB](https://github.com/ryogrid/SamehadaDB/blob/master/README.md "SamehadaDB/README.md at master · ryogrid/SamehadaDB")

Thanks :)

- [x] Predicates on Seq Scan
- [x] Multiple Item on Predicate: AND (OR is not supported yet)
- [] Predicates: <, >, <=, >=
- [] Null
- [] Predicates: NOT
- [x] Delete Tuple
- [x] Update Tuple
- [] LIMIT / OFFSET
- [x] Varchar
- [x] Persistent Catalog
- [ ] Updating of Table Schema 
- [x] Latches
- [x] Transactions
- [x] Rollback When Abort Occurs
- [x] Logging
- [x] Checkpointing
- [x] Simple Checkpointing (all transactions are blocked until finish of checkpointing)
  - [ ] Fuzzy Checkpointing (ARIES)
- [x] Recovery from Logs
- [x] Index
  - [x] Hash Index
    - Hash index can be used only equal(=) operator is specified to index having columns
    - Thread safe but serialized (not supported concurrent access)
  - [ ] SkipList Index
  - [x] B-tree Index
  - [ ] Logging And Recovery Of Index Data
- [ ] JOIN
  - [x] INNER JOIN (Hash Join, Index Join, Nested Loop Join)
    - Condition specified at ON clause should be composed of single item and can use equal(==) operator only
  - [ ] OUTER JOIN
  - [x] CROSS JOIN
- [] Aggregations (COUNT, MAX, MIN, SUM on SELECT clause including Group by and Having)
- [] Sort (ORDER BY clause)
- [x] Concurrent Execution of Transactions
- [x] Execution of Query with SQL string
  - not supported multi queries on a request yet
- [x] Frontend Impl as Embedded DB Library (like SQLite)
- [ ] Deduplication of Result Records (Distinct)
- [] Query Optimization (Selinger) 
  - cases below are not supported now
    - predicate including OR operation, NOT, IS NULL
    - projection including aggregation
    - LIMIT, ORDER BY
- [x] Statistics Data for Optimizer
- [ ] TRANSACTION Statement on SQL
  - This includes adding support of multi statements (multi statements is not suported on SQL now)
- [ ] AS clause
- [ ] Nested Query
- [ ] Predicates: IN
- [ ] DB Connector (Driver) or Other Kind of Network Access Interface
  - [ ] MySQL or PostgreSQL Compatible Protocol
  - [] REST
- [x] Deallocate and Reuse Page
- [x] Optimization of INSERT
- [ ] UNION clause
- [x] Materialization
  - Classes which offers functionality for materialization exists
  - Now, HashJoinExecutor only do materialization with the classes 
- [ ] Authentication
- [ ] Making Usable from OR Mapper of One Web Framework Such as Django (Python) on Simple Application Scope
  - implementation of DB driver/connector for Python is needed (Or supporting major RDBMS compatible NW I/F) 

## Development

### Requirements
- Rust toolchain (stable)

### Running Tests
```bash
cargo test
```

### Embedded

#### Using Samples

Sample data is stored in studentdb. If you use this database, you can try queries without creating tables and inserting data.

```
cargo run --bin embedded
```
or
```
cargo run --bin embedded -- -d studentdb
```

##### Sample queries

```sql
sabidb>select sid, sname, majorid, gradyear from students
sid     sname      majorid  gradyear  
--------------------------------------
      1 joe              10      2021 
      2 amy              20      2020 
      3 max              10      2022 
      4 sue              20      2022 
      5 bob              30      2020 
      6 kim              20      2020 
      7 art              30      2021 
      8 pat              20      2019 
      9 lee              10      2021 
transaction 2 commited
Rows: 9
```

```sql
sabidb>select sname, dname, title, prof, grade from students,depts, courses, sections, enrolls where sid=studentid and sectid=sectionid and cid=courseid and deptid=did
sname      dname     title       prof      grade  
--------------------------------------------------
joe        compsci   db systems  turing    A      
sue        math      calculus    newton    B      
joe        math      calculus    einstein  C      
amy        math      calculus    einstein  B+     
sue        drama     elocution   brando    A      
kim        drama     elocution   brando    A      
transaction 2 commited
Rows: 6
```


#### Using a new database

You can nominate your original database by `-d`.

```
cargo run --bin embedded -- -d <dbname>
ex: cargo run --bin embedded -- -d <dbname>
```

##### Create tables and data

You can find a comprehensive set of sample SQL commands to create tables and insert data in [docs/samples/setup.sql](./docs/samples/setup.sql).



## SQL Examples (Verified)

The following SQL scenarios are automatically verified by `cargo test` against both **Basic** and **Heuristic** planners, ensuring data integrity and persistence.

### 1. Data Definition (DDL)
```sql
-- Create tables, indexes, and views
create table students(sid int, sname varchar(9), majorid int, gradyear int)
create table depts(did int, dname varchar(8))
create index majorid_idx on students(majorid)
create view cs_students as select sid, sname from students where majorid = 10
```

### 2. Data Manipulation (DML)
```sql
-- Insert, Update, and Delete
insert into students(sid, sname, majorid, gradyear) values (1, 'joe', 10, 2021)
update students set gradyear = 2023 where sid = 1
delete from students where sid = 5
```

### 3. Querying
```sql
-- Basic selection
select sid, sname, majorid, gradyear from students

-- Joins, View expansion, and Multiple conditions
select sname, dname from students, depts where majorid = did
select sid, sname from cs_students
select sname, dname from students, depts where majorid = did and gradyear = 2021
```

### Tips: Switching Planners
You can choose between the simple default planner or the optimized heuristic planner by changing the initialization in your code (e.g., in `app/embedded/main.rs`):

```rust
// Use default Basic planner
let db = SimpleDB::new(dbpath);

// OR use optimized Heuristic planner (supports Index Join/Select)
let db = SimpleDB::new_with_refined_planners(dbpath);
```