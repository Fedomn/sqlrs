# sql-query-engine-rs

Take advantage of Rust to build sql query engine from scratch that including:

- declarative macro
- visitor pattern
- futures-async-stream

Some description of the project:
- Using Apache Arrow as the data format, and the query engine is built on top of it.
- Currently, the storage layer only support CSV file as data source.
- Most of idea inspired by [risinglight](https://github.com/risinglightdb/risinglight).
