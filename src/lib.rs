#![feature(generators, proc_macro_hygiene, stmt_expr_attributes)]
#![feature(generic_associated_types)]
#![feature(backtrace)]
#![feature(iterator_try_collect)]

pub mod binder;
pub mod catalog;
pub mod db;
pub mod executor;
pub mod optimizer;
pub mod parser;
pub mod planner;
pub mod storage;
pub mod types;
pub mod util;

pub use self::db::{Database, DatabaseError};
