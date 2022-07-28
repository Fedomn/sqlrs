use std::sync::Arc;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use sqlparser::parser::ParserError;

use crate::binder::{BindError, Binder};
use crate::executor::{try_collect, ExecutorBuilder, ExecutorError};
use crate::optimizer::{InputRefRewriter, PhysicalRewriter, PlanRewriter};
use crate::parser::parse;
use crate::planner::{LogicalPlanError, Planner};
use crate::storage::{CsvStorage, Storage, StorageError, StorageImpl};
use crate::util::pretty_plan_tree;

pub struct Database {
    storage: StorageImpl,
}

impl Database {
    pub fn new_on_csv() -> Self {
        let storage = Arc::new(CsvStorage::new());
        Database {
            storage: StorageImpl::CsvStorage(storage),
        }
    }

    pub fn create_csv_table(
        &self,
        table_name: String,
        filepath: String,
    ) -> Result<(), DatabaseError> {
        if let StorageImpl::CsvStorage(ref storage) = self.storage {
            storage.create_csv_table(table_name, filepath)?;
            Ok(())
        } else {
            Err(DatabaseError::InternalError(
                "currently only support csv storage".to_string(),
            ))
        }
    }

    pub async fn run(&self, sql: &str) -> Result<Vec<RecordBatch>, DatabaseError> {
        let storage = if let StorageImpl::CsvStorage(ref storage) = self.storage {
            storage
        } else {
            return Err(DatabaseError::InternalError(
                "currently only support csv storage".to_string(),
            ));
        };

        // 1. parse sql to AST
        let stats = parse(sql)?;

        // 2. bind AST to bound stmts
        let catalog = storage.get_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let bound_stmt = binder.bind(&stats[0])?;
        println!("bound_stmt = {:#?}", bound_stmt);

        // 3. convert bound stmts to logical plan
        let planner = Planner {};
        let logical_plan = planner.plan(bound_stmt)?;
        println!("logical_plan = {:#?}", logical_plan);
        pretty_plan_tree(&*logical_plan);

        let mut input_ref_rewriter = InputRefRewriter::default();
        let new_logical_plan = input_ref_rewriter.rewrite(logical_plan);
        println!("new_logical_plan = {:#?}", new_logical_plan);
        pretty_plan_tree(&*new_logical_plan);

        // 4. rewrite logical plan to physical plan
        let mut physical_rewriter = PhysicalRewriter {};
        let physical_plan = physical_rewriter.rewrite(new_logical_plan);
        println!("physical_plan = {:#?}", physical_plan);
        pretty_plan_tree(&*physical_plan);

        // 5. build executor
        let mut builder = ExecutorBuilder::new(StorageImpl::CsvStorage(storage.clone()));
        let executor = builder.build(physical_plan);

        // 6. collect result
        let output = try_collect(executor).await?;
        Ok(output)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("parse error: {0}")]
    Parse(
        #[source]
        #[from]
        ParserError,
    ),
    #[error("bind error: {0}")]
    Bind(
        #[source]
        #[from]
        BindError,
    ),
    #[error("logical plan error: {0}")]
    Plan(
        #[source]
        #[from]
        LogicalPlanError,
    ),
    #[error("execute error: {0}")]
    Execute(
        #[source]
        #[from]
        ExecutorError,
    ),
    #[error("Storage error: {0}")]
    StorageError(
        #[source]
        #[from]
        #[backtrace]
        StorageError,
    ),
    #[error("Arrow error: {0}")]
    ArrowError(
        #[source]
        #[from]
        #[backtrace]
        ArrowError,
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
}
