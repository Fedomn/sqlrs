use std::sync::Arc;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use sqlparser::parser::ParserError;

use crate::binder::{BindError, Binder};
use crate::executor::{try_collect, ExecutorBuilder, ExecutorError};
use crate::optimizer::{
    EliminateLimits, HepBatch, HepBatchStrategy, HepOptimizer, InputRefRwriteRule,
    LimitProjectTranspose, PhysicalRewriteRule, PushLimitIntoTableScan, PushLimitThroughJoin,
    PushPredicateThroughJoin, PushProjectIntoTableScan,
};
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

    pub fn show_tables(&self) -> Result<RecordBatch, DatabaseError> {
        let data = match &self.storage {
            StorageImpl::CsvStorage(s) => s.show_tables()?,
            StorageImpl::InMemoryStorage(s) => s.show_tables()?,
        };
        Ok(data)
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
        println!("bound_stmt = {:?}", bound_stmt);

        // 3. convert bound stmts to logical plan
        let planner = Planner {};
        let logical_plan = planner.plan(bound_stmt)?;
        // println!("logical_plan = {:#?}", logical_plan);
        pretty_plan_tree(&*logical_plan);

        // 4. optimize logical plan to physical plan
        let default_batch = HepBatch::new(
            "Operator push down".to_string(),
            HepBatchStrategy::fix_point_topdown(100),
            vec![
                PushPredicateThroughJoin::create(),
                LimitProjectTranspose::create(),
                PushLimitThroughJoin::create(),
                EliminateLimits::create(),
                PushLimitIntoTableScan::create(),
                PushProjectIntoTableScan::create(),
            ],
        );

        let batch = HepBatch::new(
            "Final Step".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![InputRefRwriteRule::create(), PhysicalRewriteRule::create()],
        );
        let mut optimizer = HepOptimizer::new(vec![default_batch, batch], logical_plan);
        let physical_plan = optimizer.find_best();

        // println!("physical_plan = {:#?}", physical_plan);
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
