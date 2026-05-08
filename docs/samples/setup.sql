-- Sample data setup for sabidb
-- These commands can be used to populate a new database with sample tables and data.

-- Create Tables
create table dept(did int, dname varchar(14), loc varchar(13) );
create table emp(empno int, ename varchar(10), job varchar(9), mgr int, hiredate varchar(10), sal int, comm int, deptno int);
create table bonus(ename varchar(10), job varchar(9), sal int, comm int);
create table salgrade( grade int, losal int, hisal int);

-- Insert Data for dept
insert into dept (did, dname, loc) values (10,'accounting','new york')
insert into dept (did, dname, loc) values (20,'research','dallas');
insert into dept (did, dname, loc) values (30,'sales','chicago');
insert into dept (did, dname, loc) values (40,'operations','boston');

-- Insert Data for emp
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

-- Insert Data for salgrade
insert into salgrade (grade, losal, hisal) values (1,700,1200);
insert into salgrade (grade, losal, hisal) values (2,1201,1400);
insert into salgrade (grade, losal, hisal) values (3,1401,2000);
insert into salgrade (grade, losal, hisal) values (4,2001,3000);
insert into salgrade (grade, losal, hisal) values (5,3001,9999);

-- Setup Views and Indexes
create view salgrade_tmp as select grade, losal, hisal from salgrade where grade=1
create index emp_idx on emp(empno)
