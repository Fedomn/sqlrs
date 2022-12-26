use crate::types_v2::LogicalType;

mod logical_create_table;
mod logical_dummy_scan;
mod logical_explain;
mod logical_expression_get;
mod logical_filter;
mod logical_get;
mod logical_insert;
mod logical_limit;
mod logical_projection;
use derive_new::new;
pub use logical_create_table::*;
pub use logical_dummy_scan::*;
pub use logical_explain::*;
pub use logical_expression_get::*;
pub use logical_filter::*;
pub use logical_get::*;
pub use logical_insert::*;
pub use logical_limit::*;
pub use logical_projection::*;

use super::{BoundExpression, ColumnBinding};

#[derive(new, Default, Debug, Clone)]
pub struct LogicalOperatorBase {
    pub(crate) children: Vec<LogicalOperator>,
    // The set of expressions contained within the operator, if any
    pub(crate) expressioins: Vec<BoundExpression>,
    /// The types returned by this logical operator.
    pub(crate) types: Vec<LogicalType>,
}

#[derive(Debug, Clone)]
pub enum LogicalOperator {
    LogicalCreateTable(LogicalCreateTable),
    LogicalDummyScan(LogicalDummyScan),
    LogicalExpressionGet(LogicalExpressionGet),
    LogicalInsert(LogicalInsert),
    LogicalGet(LogicalGet),
    LogicalProjection(LogicalProjection),
    LogicalExplain(LogicalExplain),
    LogicalFilter(LogicalFilter),
    LogicalLimit(LogicalLimit),
}

impl LogicalOperator {
    pub fn children_mut(&mut self) -> &mut [LogicalOperator] {
        match self {
            LogicalOperator::LogicalCreateTable(op) => &mut op.base.children,
            LogicalOperator::LogicalExpressionGet(op) => &mut op.base.children,
            LogicalOperator::LogicalInsert(op) => &mut op.base.children,
            LogicalOperator::LogicalGet(op) => &mut op.base.children,
            LogicalOperator::LogicalProjection(op) => &mut op.base.children,
            LogicalOperator::LogicalDummyScan(op) => &mut op.base.children,
            LogicalOperator::LogicalExplain(op) => &mut op.base.children,
            LogicalOperator::LogicalFilter(op) => &mut op.base.children,
            LogicalOperator::LogicalLimit(op) => &mut op.base.children,
        }
    }

    pub fn children(&self) -> &[LogicalOperator] {
        match self {
            LogicalOperator::LogicalCreateTable(op) => &op.base.children,
            LogicalOperator::LogicalExpressionGet(op) => &op.base.children,
            LogicalOperator::LogicalInsert(op) => &op.base.children,
            LogicalOperator::LogicalGet(op) => &op.base.children,
            LogicalOperator::LogicalProjection(op) => &op.base.children,
            LogicalOperator::LogicalDummyScan(op) => &op.base.children,
            LogicalOperator::LogicalExplain(op) => &op.base.children,
            LogicalOperator::LogicalFilter(op) => &op.base.children,
            LogicalOperator::LogicalLimit(op) => &op.base.children,
        }
    }

    pub fn add_child(&mut self, child: LogicalOperator) {
        match self {
            LogicalOperator::LogicalCreateTable(op) => op.base.children.push(child),
            LogicalOperator::LogicalExpressionGet(op) => op.base.children.push(child),
            LogicalOperator::LogicalInsert(op) => op.base.children.push(child),
            LogicalOperator::LogicalGet(op) => op.base.children.push(child),
            LogicalOperator::LogicalProjection(op) => op.base.children.push(child),
            LogicalOperator::LogicalDummyScan(op) => op.base.children.push(child),
            LogicalOperator::LogicalExplain(op) => op.base.children.push(child),
            LogicalOperator::LogicalFilter(op) => op.base.children.push(child),
            LogicalOperator::LogicalLimit(op) => op.base.children.push(child),
        }
    }

    pub fn expressions(&mut self) -> &mut [BoundExpression] {
        match self {
            LogicalOperator::LogicalCreateTable(op) => &mut op.base.expressioins,
            LogicalOperator::LogicalExpressionGet(op) => &mut op.base.expressioins,
            LogicalOperator::LogicalInsert(op) => &mut op.base.expressioins,
            LogicalOperator::LogicalGet(op) => &mut op.base.expressioins,
            LogicalOperator::LogicalProjection(op) => &mut op.base.expressioins,
            LogicalOperator::LogicalDummyScan(op) => &mut op.base.expressioins,
            LogicalOperator::LogicalExplain(op) => &mut op.base.expressioins,
            LogicalOperator::LogicalFilter(op) => &mut op.base.expressioins,
            LogicalOperator::LogicalLimit(op) => &mut op.base.expressioins,
        }
    }

    pub fn types(&self) -> &[LogicalType] {
        match self {
            LogicalOperator::LogicalCreateTable(op) => &op.base.types,
            LogicalOperator::LogicalExpressionGet(op) => &op.base.types,
            LogicalOperator::LogicalInsert(op) => &op.base.types,
            LogicalOperator::LogicalGet(op) => &op.base.types,
            LogicalOperator::LogicalProjection(op) => &op.base.types,
            LogicalOperator::LogicalDummyScan(op) => &op.base.types,
            LogicalOperator::LogicalExplain(op) => &op.base.types,
            LogicalOperator::LogicalFilter(op) => &op.base.types,
            LogicalOperator::LogicalLimit(op) => &op.base.types,
        }
    }

    pub fn get_column_bindings(&self) -> Vec<ColumnBinding> {
        let default = vec![ColumnBinding::new(0, 0)];
        match self {
            LogicalOperator::LogicalCreateTable(_) => default,
            LogicalOperator::LogicalExpressionGet(op) => {
                self.generate_column_bindings(op.table_idx, op.expr_types.len())
            }
            LogicalOperator::LogicalInsert(_) => default,
            LogicalOperator::LogicalGet(op) => {
                self.generate_column_bindings(op.table_idx, op.returned_types.len())
            }
            LogicalOperator::LogicalProjection(op) => {
                self.generate_column_bindings(op.table_idx, op.base.expressioins.len())
            }
            LogicalOperator::LogicalDummyScan(op) => vec![ColumnBinding::new(op.table_idx, 0)],
            LogicalOperator::LogicalExplain(_) => {
                vec![ColumnBinding::new(0, 0), ColumnBinding::new(0, 1)]
            }
            LogicalOperator::LogicalFilter(op) => op.base.children[0].get_column_bindings(),
            LogicalOperator::LogicalLimit(op) => op.base.children[0].get_column_bindings(),
        }
    }

    pub fn resolve_operator_types(&mut self) {
        for child in self.children_mut() {
            child.resolve_operator_types();
        }
        match self {
            LogicalOperator::LogicalCreateTable(op) => {
                op.base.types.push(LogicalType::Bigint);
            }
            LogicalOperator::LogicalExpressionGet(op) => {
                op.base.types = op.expr_types.clone();
            }
            LogicalOperator::LogicalInsert(op) => op.base.types.push(LogicalType::Bigint),
            LogicalOperator::LogicalGet(op) => op.base.types.extend(op.returned_types.clone()),
            LogicalOperator::LogicalProjection(op) => {
                let types = op
                    .base
                    .expressioins
                    .iter()
                    .map(|e| e.return_type())
                    .collect::<Vec<_>>();
                op.base.types.extend(types);
            }
            LogicalOperator::LogicalDummyScan(op) => op.base.types.push(LogicalType::Integer),
            LogicalOperator::LogicalExplain(op) => {
                op.base.types = vec![LogicalType::Varchar, LogicalType::Varchar];
            }
            LogicalOperator::LogicalFilter(op) => {
                op.base.types = op.base.children[0].types().to_vec();
            }
            LogicalOperator::LogicalLimit(op) => {
                op.base.types = op.base.children[0].types().to_vec();
            }
        }
    }

    fn generate_column_bindings(
        &self,
        table_idx: usize,
        column_count: usize,
    ) -> Vec<ColumnBinding> {
        let mut result = vec![];
        for idx in 0..column_count {
            result.push(ColumnBinding::new(table_idx, idx))
        }
        result
    }
}
