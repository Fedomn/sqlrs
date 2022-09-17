# sql-query-engine-rs

Take advantage of Rust to build sql query engine from scratch that including:

- declarative macro
- visitor pattern
- futures-async-stream

Some description of the project:
- The goal of this project is to build a modern sql query engine for learning purpose, including detailed feature tracking roadmaps and blog posts.
- Using Apache Arrow as the data format, and the query engine is built on top of it.
- Currently, the storage layer only support CSV file as data source.
- Most of idea inspired by [risinglight](https://github.com/risinglightdb/risinglight) and [datafusion](https://github.com/apache/arrow-datafusion)

# SQL demo

currently, the following SQL statements are supported, execute `make run` into interactive mode to test them:

```sql
-- supported in Roadmap 0.1
select first_name from employee where last_name = 'Hopkins';

-- supported in Roadmap 0.2
select sum(salary+1), count(salary), max(salary) from employee where id > 1;
select state, count(state), sum(salary) from employee group by state;
-- load csv table
\load csv department ./tests/csv/department.csv
\load csv employee ./tests/csv/employee.csv
-- show tables
\dt

-- supported in Roadmap 0.3
select id from employee order by id desc offset 2 limit 1;
select * from employee left join state on employee.state=state.state_code and state.state_name!='California State';

-- supported in Roadmap 0.4
-- explain plan tree
\explain select a from t1;
-- Heuristic Optimizer that includes rules such as: Column pruning, Predicates pushdown, Limit pushdown etc.
```


# Roadmap

High level description:

- Roadmap 0.1: Build a basic SQL query on CSV storage
- Roadmap 0.2: Support aggregation operators, e2e testing framework and interactive mode
- Roadmap 0.3: Support limit, order, and join operators
- Roadmap 0.4: Introduce a Heuristic Optimizer and common optimization rules

Please see [Roadmap](https://github.com/Fedomn/sql-query-engine-rs/issues?q=roadmap) for more information of implementation steps


# Deep Dive Series Blog (in Chinese)

On my blog:

- [Part 1 for Roadmap 0.1 and 0.2](https://frankma.me/posts/database/sql-query-engine-rs-part-1/)
- [Part 2 for Roadmap 0.3](https://frankma.me/posts/database/sql-query-engine-rs-part-2/)
- [Part 3 for Roadmap 0.4](https://frankma.me/posts/database/sql-query-engine-rs-part-3/)
