#![feature(generators, proc_macro_hygiene, stmt_expr_attributes)]
#![feature(generic_associated_types)]
#![feature(backtrace)]
#![feature(iterator_try_collect)]
use std::sync::Arc;

use anyhow::Result;

use crate::binder::Binder;
use crate::executor::{pretty_batches, try_collect, ExecutorBuilder};
use crate::optimizer::{InputRefRewriter, PhysicalRewriter, PlanRewriter};
use crate::parser::parse;
use crate::planner::Planner;
use crate::storage::{CsvStorage, Storage, StorageImpl};

mod binder;
mod catalog;
mod executor;
mod optimizer;
mod parser;
mod planner;
mod storage;
mod types;

#[tokio::main]
async fn main() -> Result<()> {
    let id = "employee".to_string();
    let filepath = "./tests/employee.csv".to_string();
    let storage = CsvStorage::new();
    storage.create_table(id.clone(), filepath)?;

    // 1. parse sql to AST
    let stats = parse("select first_name from employee where last_name = 'Hopkins'").unwrap();

    // 2. bind AST to bound stmts
    let catalog = storage.get_catalog();
    let mut binder = Binder::new(Arc::new(catalog));
    let bound_stmt = binder.bind(&stats[0]).unwrap();
    println!("bound_stmt = {:#?}", bound_stmt);

    // 3. convert bound stmts to logical plan
    let planner = Planner {};
    let logical_plan = planner.plan(bound_stmt)?;
    println!("logical_plan = {:#?}", logical_plan);
    let mut input_ref_rewriter = InputRefRewriter::default();
    let new_logical_plan = input_ref_rewriter.rewrite(logical_plan);
    println!("new_logical_plan = {:#?}", new_logical_plan);

    // 4. rewrite logical plan to physical plan
    let mut physical_rewriter = PhysicalRewriter {};
    let physical_plan = physical_rewriter.rewrite(new_logical_plan);
    println!("physical_plan = {:#?}", physical_plan);

    // 5. build executor
    let mut builder = ExecutorBuilder::new(StorageImpl::CsvStorage(Arc::new(storage)));
    let executor = builder.build(physical_plan);

    // 6. collect result
    let output = try_collect(executor).await?;
    pretty_batches(&output);
    Ok(())
}
