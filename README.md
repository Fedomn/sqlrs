# sql-query-engine-rs

Take advantage of Rust to build sql query engine from scratch that including:

- declarative macro
- visitor pattern
- futures-async-stream

Some description of the project:
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
```


# Roadmap

Please see [Roadmap](https://github.com/Fedomn/sql-query-engine-rs/issues?q=roadmap) for more information of implementation steps


# Deep Dive Series Blog (in Chinese)

On my blog:

- [Part 1 for Roadmap 0.1 and 0.2](https://frankma.me/posts/database/sql-query-engine-rs-part-1/)
- [Part 2 for Roadmap 0.3](https://frankma.me/posts/database/sql-query-engine-rs-part-2/)
