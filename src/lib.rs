#![feature(generators, proc_macro_hygiene, stmt_expr_attributes)]
#![feature(generic_associated_types)]
#![feature(backtrace)]
#![feature(iterator_try_collect)]
#![feature(assert_matches)]

#[macro_use]
extern crate lazy_static;

pub mod binder;
pub mod catalog;
pub mod catalog_v2;
pub mod cli;
pub mod db;
pub mod execution;
pub mod executor;
pub mod main_entry;
pub mod optimizer;
pub mod parser;
pub mod planner;
pub mod planner_v2;
pub mod storage;
pub mod storage_v2;
pub mod types;
pub mod types_v2;
pub mod util;

pub use self::db::{Database, DatabaseError};
