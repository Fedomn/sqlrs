#![feature(generators, proc_macro_hygiene, stmt_expr_attributes)]
#![feature(generic_associated_types)]
#![feature(backtrace)]
use std::sync::Arc;

use anyhow::Result;

use crate::binder::Binder;
use crate::executor::{pretty_batches, try_collect, ExecutorBuilder};
use crate::optimizer::{PhysicalRewriter, PlanRewriter};
use crate::parser::parse;
use crate::planner::Planner;
use crate::storage::{CsvStorage, Storage, StorageImpl, Table, Transaction};

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
    let table = storage.get_table(id)?;
    let mut tx = table.read()?;

    let mut total_cnt = 0;
    loop {
        let batch = tx.next_batch()?;
        match batch {
            Some(batch) => total_cnt += batch.num_rows(),
            None => break,
        }
    }
    println!("total_cnt = {:?}", total_cnt);
    let catalog = storage.get_catalog();
    let mut binder = Binder::new(Arc::new(catalog));
    // let stats = parse("select first_name from employee where last_name = 'Hopkins'").unwrap();
    let stats = parse("select first_name from employee").unwrap();
    let bound_stmt = binder.bind(&stats[0]).unwrap();
    println!("bound_stmt = {:#?}", bound_stmt);
    let planner = Planner {};
    let logical_plan = planner.plan(bound_stmt)?;
    println!("logical_plan = {:#?}", logical_plan);
    let mut physical_rewriter = PhysicalRewriter {};
    let physical_plan = physical_rewriter.rewrite(logical_plan);
    println!("physical_plan = {:#?}", physical_plan);
    let mut builder = ExecutorBuilder::new(StorageImpl::CsvStorage(Arc::new(storage)));
    let executor = builder.build(physical_plan);
    let output = try_collect(executor).await?;
    pretty_batches(&output);
    Ok(())
}
