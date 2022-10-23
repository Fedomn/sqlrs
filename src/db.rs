use std::fmt::Write;
use std::sync::Arc;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use sqlparser::parser::ParserError;

use crate::binder::{BindError, Binder};
use crate::executor::{try_collect, ExecutorBuilder, ExecutorError};
use crate::optimizer::{
    CollapseProject, CombineFilter, EliminateLimits, HepBatch, HepBatchStrategy, HepOptimizer,
    InputRefRewriter, LimitProjectTranspose, PhysicalRewriteRule, PlanRef, PlanRewriter,
    PushLimitIntoTableScan, PushLimitThroughJoin, PushPredicateThroughJoin,
    PushPredicateThroughNonJoin, PushProjectIntoTableScan, PushProjectThroughChild,
    RemoveNoopOperators, SimplifyCasts,
};
use crate::parser::parse;
use crate::planner::{LogicalPlanError, Planner};
use crate::storage::{CsvStorage, Storage, StorageError, StorageImpl};
use crate::util::pretty_plan_tree_string;

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

    fn default_optimizer(&self, root: PlanRef) -> HepOptimizer {
        // the order of rules is important and affects the rule matching logic
        let batches = vec![
            HepBatch::new(
                "Predicate pushdown".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    PushPredicateThroughNonJoin::create(),
                    PushPredicateThroughJoin::create(),
                ],
            ),
            HepBatch::new(
                "Limit pushdown".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    LimitProjectTranspose::create(),
                    PushLimitThroughJoin::create(),
                    PushLimitIntoTableScan::create(),
                    EliminateLimits::create(),
                ],
            ),
            HepBatch::new(
                "Column pruning".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    PushProjectThroughChild::create(),
                    PushProjectIntoTableScan::create(),
                    RemoveNoopOperators::create(),
                ],
            ),
            HepBatch::new(
                "Combine operators".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![CollapseProject::create(), CombineFilter::create()],
            ),
            HepBatch::new(
                "One-time simplification".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![SimplifyCasts::create()],
            ),
            HepBatch::new(
                "Rewrite physical plan".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![PhysicalRewriteRule::create()],
            ),
        ];

        HepOptimizer::new(batches, root)
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
        println!("bound_stmt:\n{:#?}\n", bound_stmt);

        // 3. convert bound stmts to logical plan
        let mut planner = Planner::default();
        let logical_plan = planner.plan(bound_stmt)?;
        println!(
            "original_plan:\n{}\n",
            pretty_plan_tree_string(&*logical_plan)
        );

        // 4. optimize logical plan to physical plan
        let mut optimizer = self.default_optimizer(logical_plan);
        let physical_plan = optimizer.find_best();
        println!(
            "optimized_plan:\n{}\n",
            pretty_plan_tree_string(&*physical_plan)
        );

        // 5. build executor
        let mut builder = ExecutorBuilder::new(StorageImpl::CsvStorage(storage.clone()));
        let mut rewriter = InputRefRewriter::default();
        let physical_plan = rewriter.rewrite(physical_plan);
        let executor = builder.build(physical_plan);

        // 6. collect result
        let output = try_collect(executor).await?;
        Ok(output)
    }

    pub async fn explain(&self, sql: &str) -> Result<String, DatabaseError> {
        let storage = if let StorageImpl::CsvStorage(ref storage) = self.storage {
            storage
        } else {
            return Err(DatabaseError::InternalError(
                "currently only support csv storage".to_string(),
            ));
        };

        let stats = parse(sql)?;

        let catalog = storage.get_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let bound_stmt = binder.bind(&stats[0])?;

        let mut explain_str = String::new();
        let mut planner = Planner::default();
        let logical_plan = planner.plan(bound_stmt)?;
        _ = write!(
            explain_str,
            "original plan:\n{}\n",
            pretty_plan_tree_string(&*logical_plan)
        );

        let mut optimizer = self.default_optimizer(logical_plan);
        let physical_plan = optimizer.find_best();
        _ = write!(
            explain_str,
            "optimized plan:\n{}\n",
            pretty_plan_tree_string(&*physical_plan)
        );

        Ok(explain_str)
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
