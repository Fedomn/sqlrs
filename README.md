# sqlrs

sqlrs is an in-process sql query engine that designed for OLAP

- The goal of this project is to build a embedded in-process sql query engine for OLAP workloads.
- It leverages the power of Rust ecosystem and Apache Arrow.
- It achieved columnar-vectorized execution engine.
- It will support pipeline parallelism execution in the future.

🚧 The project is actively developing a new planner V2 that inspired by DuckDB, and will replace the planner v1 in the future.🚧 

# SQL demo

currently, the following SQL statements are supported, execute the commands into interactive mode to test them:

- `make run`: run sqlrs in planner_v1
- `make run_v2`: run sqlrs in planner_v2

```sql
-- supported in Roadmap 0.1 (planner_v1)
select first_name from employee where last_name = 'Hopkins';

-- supported in Roadmap 0.2 (planner_v1)
select sum(salary+1), count(salary), max(salary) from employee where id > 1;
select state, count(state), sum(salary) from employee group by state;
-- load csv table
\load csv department ./tests/csv/department.csv
\load csv employee ./tests/csv/employee.csv
-- show tables
\dt

-- supported in Roadmap 0.3 (planner_v1)
select id from employee order by id desc offset 2 limit 1;
select * from employee left join state on employee.state=state.state_code and state.state_name!='California State';

-- supported in Roadmap 0.4 (planner_v1)
-- explain plan tree
\explain select a from t1;
-- Heuristic Optimizer that includes rules such as: Column pruning, Predicates pushdown, Limit pushdown etc.

-- supported in Roadmap 0.5 (planner_v1)
-- distinct
select distinct state from employee;
select count(distinct(b)) from t2;
-- alias
select a as c1 from t1 order by c1 desc limit 1;
select t.a from t1 t where t.b > 1 order by t.a desc limit 1;
-- uncorrelated scalar subquery
select t.* from (select * from t1 where a > 1) t where t.b > 7;
select a, (select max(b) from t1) max_b from t1;


-- supported in Roadmap 0.6 (planner_v2)
-- create and insert table in memory
create table t1(v1 int, v2 int, v3 int);
insert into t1 values (0, 4, 1), (1, 5, 2);
select * from t1;
-- select only expressions
select 1, 2.3, '😇', true, null;
-- pragma commands
show tables;
describe t1;
-- previous SQL statements
select v1+1 as a from t1 where a >= 2;
select v1 from t1 limit 2 offset 1;
```


# Roadmap

High level description:

- Roadmap 0.1: Build a basic SQL query on CSV storage
- Roadmap 0.2: Support aggregation operators, e2e testing framework and interactive mode
- Roadmap 0.3: Support limit, order, and join operators
- Roadmap 0.4: Introduce a Heuristic Optimizer and common optimization rules
- Roadmap 0.5: Support distinct, alias, and uncorrelated scalar subquery
- Roadmap 0.6: New planner_v2 highly inspired by DuckDB

Please see [Roadmap](https://github.com/Fedomn/sqlrs/issues?q=roadmap) for more information of implementation steps


# Deep Dive Series Blog (in Chinese)

- On my [blog](https://frankma.me/categories/sqlrs/)
- On [zhihu](https://www.zhihu.com/column/c_1554474699211628544)
