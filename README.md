# sabidb

```
 ___    __    ____  ____  ____  ____ 
/ __)  /__\  (  _ \(_  _)(  _ \(  _ \
\__ \ /(__)\  ) _ < _)(_  )(_) )) _ <
(___/(__)(__)(____/(____)(____/(____/
```

Rust implementation of SimpleDB from [Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7 "Database Design and Implementation")

This application is only for local runtime.

![sabidb_basic](./sabidb_cropped.gif 'sabidb_basic')

## Roadmap

This roadmap is referred to  [SamehadaDB/README.md at master · ryogrid/SamehadaDB](https://github.com/ryogrid/SamehadaDB/blob/master/README.md "SamehadaDB/README.md at master · ryogrid/SamehadaDB")

Thanks :)

- [x] Predicates on Seq Scan
- [x] Multiple Item on Predicate: AND, OR
- [] Predicates: <, >, <=, >=
- [] Null
- [] Predicates: NOT
- [] Delete Tuple
- [] Update Tuple
- [] LIMIT / OFFSET
- [x] Varchar
- [] Persistent Catalog
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
  - [x] SkipList Index
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
- [] Statistics Data for Optimizer
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

## How to try

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


```sql
create table dept(did int, dname varchar(14), loc varchar(13) );
create table emp(empno int, ename varchar(10), job varchar(9), mgr int, hiredate varchar(10), sal int, comm int, deptno int);
create table bonus(ename varchar(10), job varchar(9), sal int, comm int);
create table salgrade( grade int, losal int, hisal int);
insert into dept (did, dname, loc) values (10,'accounting','new york')
insert into dept (did, dname, loc) values (20,'research','dallas');
insert into dept (did, dname, loc) values (30,'sales','chicago');
insert into dept (did, dname, loc) values (40,'operations','boston');
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno) values (7369,'smith','clerk',7902,'17-12-1980',800,300,20);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7499,'allen','salesman',7698,'20-2-1981',1600,300,30);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7521,'ward','salesman',7698,'22-2-1981',1250,500,30);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7566,'jones','manager',7839,'2-4-1981',2975,500,20);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7654,'martin','salesman',7698,'28-9-1981',1250,1400,30);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7698,'blake','manager',7839,'1-5-1981',2850,1400,30);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7782,'clark','manager',7839,'9-6-1981',2450,1400,10);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7788,'scott','analyst',7566,'13-07-87',-85,3000,1400,20);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7839,'king','president',7566,'17-11-1981',5000,1400,10);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7844,'turner','salesman',7698,'8-9-1981',1500,0,30);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7876,'adams','clerk',7788,'13-07-87' -51,1100,0,20);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7900,'james','clerk',7698,'3-12-1981',950,0,30);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7902,'ford','analyst',7566,'3-12-1981',3000,0,20);
insert into emp (empno , ename , job, mgr, hiredate , sal, comm, deptno)  values (7934,'miller','clerk',7782,'23-1-1982',1300,0,10);
insert into salgrade (grade, losal, hisal) values (1,700,1200);
insert into salgrade (grade, losal, hisal) values (2,1201,1400);
insert into salgrade (grade, losal, hisal) values (3,1401,2000);
insert into salgrade (grade, losal, hisal) values (4,2001,3000);
insert into salgrade (grade, losal, hisal) values (5,3001,9999);

create view salgrade_tmp as select grade, losal, hisal from salgrade where grade=1
create index emp_idx on emp(empno)
```


##### Query sample

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