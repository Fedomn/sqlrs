mod physical_column_data_scan;
mod physical_create_table;
mod physical_dummy_scan;
mod physical_explain;
mod physical_expression_scan;
mod physical_filter;
mod physical_insert;
mod physical_limit;
mod physical_projection;
mod physical_table_scan;
mod pipeline_event;
mod pipeline_operator;
mod result_type;
mod state;

use derive_new::new;
pub use physical_column_data_scan::*;
pub use physical_create_table::*;
pub use physical_dummy_scan::*;
pub use physical_explain::*;
pub use physical_expression_scan::*;
pub use physical_filter::*;
pub use physical_insert::*;
pub use physical_limit::*;
pub use physical_projection::*;
pub use physical_table_scan::*;
pub use pipeline_event::*;
pub use pipeline_operator::*;
pub use result_type::*;
pub use state::*;

use super::{MetaPipeline, Pipeline};
use crate::planner_v2::BoundExpression;

#[derive(new, Default, Clone)]
pub struct PhysicalOperatorBase {
    pub(crate) children: Vec<PhysicalOperator>,
    // The set of expressions contained within the operator, if any
    pub(crate) expressioins: Vec<BoundExpression>,
}

#[derive(Clone)]
pub enum PhysicalOperator {
    PhysicalCreateTable(PhysicalCreateTable),
    PhysicalDummyScan(PhysicalDummyScan),
    PhysicalExpressionScan(PhysicalExpressionScan),
    PhysicalInsert(Box<PhysicalInsert>),
    PhysicalTableScan(PhysicalTableScan),
    PhysicalProjection(PhysicalProjection),
    PhysicalColumnDataScan(PhysicalColumnDataScan),
    PhysicalFilter(PhysicalFilter),
    PhysicalLimit(PhysicalLimit),
}

impl PhysicalOperator {
    pub fn children(&self) -> &[PhysicalOperator] {
        match self {
            PhysicalOperator::PhysicalCreateTable(op) => &op.base.children,
            PhysicalOperator::PhysicalExpressionScan(op) => &op.base.children,
            PhysicalOperator::PhysicalInsert(op) => &op.base.children,
            PhysicalOperator::PhysicalTableScan(op) => &op.base.children,
            PhysicalOperator::PhysicalProjection(op) => &op.base.children,
            PhysicalOperator::PhysicalDummyScan(op) => &op.base.children,
            PhysicalOperator::PhysicalColumnDataScan(op) => &op.base.children,
            PhysicalOperator::PhysicalFilter(op) => &op.base.children,
            PhysicalOperator::PhysicalLimit(op) => &op.base.children,
        }
    }

    pub fn is_sink(&self) -> bool {
        match self {
            PhysicalOperator::PhysicalCreateTable(_) => true,
            PhysicalOperator::PhysicalExpressionScan(_) => true,
            PhysicalOperator::PhysicalInsert(_) => true,
            PhysicalOperator::PhysicalTableScan(_) => true,
            PhysicalOperator::PhysicalProjection(_) => false,
            PhysicalOperator::PhysicalDummyScan(_) => false,
            PhysicalOperator::PhysicalColumnDataScan(_) => false,
            PhysicalOperator::PhysicalFilter(_) => false,
            PhysicalOperator::PhysicalLimit(_) => true,
        }
    }

    pub fn build_pipeline(&self, current: &mut Pipeline, meta_pipeline: &MetaPipeline) {
        if self.is_sink() {
            assert_eq!(self.children().len(), 1);
            // single operator: the operator becomes the data source of the current pipeline
            current.source = Some(self.clone());
            // we create a new pipeline starting from the child
            // meta_pipeline.create_child_meta_pipeline()
        } else {
        }
    }
}
