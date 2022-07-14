mod project;
mod table_scan;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use futures_async_stream::try_stream;

use self::project::ProjectExecutor;
use self::table_scan::TableScanExecutor;
use crate::optimizer::{PhysicalProject, PhysicalTableScan, PlanRef, PlanTreeNode, PlanVisitor};
use crate::storage::{StorageError, StorageImpl};

pub type BoxedExecutor = BoxStream<'static, Result<RecordBatch, ExecutorError>>;

pub struct ExecutorBuilder {
    storage: StorageImpl,
}

impl ExecutorBuilder {
    pub fn new(storage: StorageImpl) -> Self {
        Self { storage }
    }

    pub fn build(&mut self, plan: PlanRef) -> BoxedExecutor {
        self.visit(plan).unwrap()
    }

    #[allow(dead_code)]
    pub fn try_collect(
        &mut self,
        plan: PlanRef,
    ) -> BoxStream<'static, Result<RecordBatch, ExecutorError>> {
        self.visit(plan).unwrap()
    }
}

pub async fn try_collect(mut executor: BoxedExecutor) -> Result<Vec<RecordBatch>, ExecutorError> {
    let mut output = Vec::new();
    while let Some(batch) = executor.try_next().await? {
        output.push(batch);
    }
    Ok(output)
}

pub fn pretty_batches(batches: &Vec<RecordBatch>) {
    _ = print_batches(batches.as_slice());
}

/// The error type of execution.
#[derive(thiserror::Error, Debug)]
pub enum ExecutorError {
    #[error("storage error: {0}")]
    Storage(
        #[from]
        #[backtrace]
        #[source]
        StorageError,
    ),
}

impl PlanVisitor<BoxedExecutor> for ExecutorBuilder {
    fn visit_physical_table_scan(&mut self, plan: &PhysicalTableScan) -> Option<BoxedExecutor> {
        Some(match &self.storage {
            StorageImpl::CsvStorage(storage) => TableScanExecutor {
                plan: plan.clone(),
                storage: storage.clone(),
            }
            .execute(),
        })
    }

    fn visit_physical_project(&mut self, plan: &PhysicalProject) -> Option<BoxedExecutor> {
        Some(
            ProjectExecutor {
                exprs: plan.logical().exprs(),
                child: self
                    .visit(plan.children().first().unwrap().clone())
                    .unwrap(),
            }
            .execute(),
        )
    }

    fn visit_physical_filter(
        &mut self,
        _plan: &crate::optimizer::PhysicalFilter,
    ) -> Option<BoxedExecutor> {
        unimplemented!(
            "The {} is not implemented visitor yet",
            stringify!(PhysicalFilter)
        )
    }
}

#[cfg(test)]
mod executor_test {
    use std::sync::Arc;

    use anyhow::Result;

    use crate::binder::Binder;
    use crate::executor::{pretty_batches, try_collect, ExecutorBuilder};
    use crate::optimizer::{InputRefRewriter, PhysicalRewriter, PlanRewriter};
    use crate::parser::parse;
    use crate::planner::Planner;
    use crate::storage::{CsvStorage, Storage, StorageImpl};

    #[tokio::test]
    async fn test_executor_works() -> Result<()> {
        // create csv storage
        let id = "employee".to_string();
        let filepath = "./tests/employee.csv".to_string();
        let storage = CsvStorage::new();
        storage.create_table(id.clone(), filepath)?;
        // parse sql
        let stmts = parse("select first_name, job_title from employee").unwrap();
        // bind to stmts
        let catalog = storage.get_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let bound_stmt = binder.bind(&stmts[0]).unwrap();
        println!("bound_stmt = {:#?}", bound_stmt);
        // convert to logical plan
        let planner = Planner {};
        let logical_plan = planner.plan(bound_stmt)?;
        println!("logical_plan = {:#?}", logical_plan);
        let mut input_ref_rewriter = InputRefRewriter::default();
        let new_logical_plan = input_ref_rewriter.rewrite(logical_plan);
        println!("new_logical_plan = {:#?}", new_logical_plan);
        // rewrite to physical plan
        let mut physical_rewriter = PhysicalRewriter {};
        let physical_plan = physical_rewriter.rewrite(new_logical_plan);
        println!("physical_plan = {:#?}", physical_plan);
        // build executor
        let mut builder = ExecutorBuilder::new(StorageImpl::CsvStorage(Arc::new(storage)));
        let executor = builder.build(physical_plan);
        // collect result
        let output = try_collect(executor).await?;
        pretty_batches(&output);
        Ok(())
    }
}
