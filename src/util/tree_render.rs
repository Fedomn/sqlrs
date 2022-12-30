use std::fmt::Write;

use derive_new::new;

use crate::catalog_v2::ColumnDefinition;
use crate::execution::PhysicalOperator;
use crate::function::FunctionData;
use crate::planner_v2::{BoundExpression, LogicalOperator};

#[derive(new)]
pub struct TreeRender;

impl TreeRender {
    fn column_definition_to_string(column: &ColumnDefinition) -> String {
        format!("{}({:?})", column.name, column.ty)
    }

    fn bound_expression_to_string(expr: &BoundExpression) -> String {
        match expr {
            BoundExpression::BoundColumnRefExpression(e) => {
                format!(
                    "ColumnRef({}[{}.{}]))",
                    e.base.alias, e.binding.table_idx, e.binding.column_idx
                )
            }
            BoundExpression::BoundConstantExpression(e) => {
                format!("Constant({})", e.value)
            }
            BoundExpression::BoundReferenceExpression(e) => {
                format!("Reference({}[{}])", e.base.alias, e.index)
            }
            BoundExpression::BoundCastExpression(e) => {
                format!(
                    "Cast({}[{}],{:?})",
                    e.base.alias,
                    Self::bound_expression_to_string(&e.child),
                    e.base.return_type,
                )
            }
            BoundExpression::BoundFunctionExpression(e) => {
                let args = e
                    .children
                    .iter()
                    .map(Self::bound_expression_to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({}])", e.function.name, args)
            }
            BoundExpression::BoundComparisonExpression(e) => {
                let l = Self::bound_expression_to_string(&e.left);
                let r = Self::bound_expression_to_string(&e.right);
                format!("{} {} {}", l, e.function.name, r)
            }
            BoundExpression::BoundConjunctionExpression(e) => {
                let args = e
                    .children
                    .iter()
                    .map(Self::bound_expression_to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({}])", e.function.name, args)
            }
        }
    }

    fn logical_plan_to_string(plan: &LogicalOperator) -> String {
        match plan {
            LogicalOperator::LogicalCreateTable(op) => {
                let table = format!("{}.{}", op.info.base.base.schema, op.info.base.table);
                let columns = op
                    .info
                    .base
                    .columns
                    .iter()
                    .map(Self::column_definition_to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("LogicalCreateTable: {}[{}]", table, columns)
            }
            LogicalOperator::LogicalDummyScan(_) => "LogicalDummyScan".to_string(),
            LogicalOperator::LogicalExpressionGet(_) => "LogicalExpressionGet".to_string(),
            LogicalOperator::LogicalInsert(op) => {
                format!(
                    "LogicalInsert: {}.{}",
                    op.table.storage.info.schema, op.table.storage.info.table
                )
            }
            LogicalOperator::LogicalGet(op) => {
                let get_table_str = match &op.bind_data {
                    Some(data) => match data {
                        FunctionData::SeqTableScanInputData(input) => {
                            format!(
                                "{}.{}",
                                input.bind_table.storage.info.schema,
                                input.bind_table.storage.info.table
                            )
                        }
                        FunctionData::SqlrsColumnsData(_) => "sqlrs_columns".to_string(),
                        FunctionData::SqlrsTablesData(_) => "sqlrs_tables".to_string(),
                        // FunctionData::ReadCSVInputData(_) => "read_csv".to_string(),
                    },
                    None => "None".to_string(),
                };
                format!("LogicalGet: {}", get_table_str)
            }
            LogicalOperator::LogicalProjection(op) => {
                let exprs = op
                    .base
                    .expressioins
                    .iter()
                    .map(Self::bound_expression_to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("LogicalProjection: {}", exprs)
            }
            LogicalOperator::LogicalExplain(_) => "LogicalExplain".to_string(),
            LogicalOperator::LogicalFilter(op) => {
                let exprs = op
                    .base
                    .expressioins
                    .iter()
                    .map(Self::bound_expression_to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("LogicalFilter: {}", exprs)
            }
            LogicalOperator::LogicalLimit(op) => {
                let limit = op
                    .limit
                    .as_ref()
                    .map(|_| format!("{}", op.limit_value))
                    .unwrap_or_else(|| "None".to_string());

                let offset = op
                    .offset
                    .as_ref()
                    .map(|_| format!("{}", op.offsert_value))
                    .unwrap_or_else(|| "None".to_string());
                format!("LogicalLimit: limit[{}], offset[{}]", limit, offset)
            }
        }
    }

    fn logical_plan_tree_internal(
        plan: &LogicalOperator,
        level: usize,
        explain_result: &mut dyn Write,
    ) {
        let plan_string = Self::logical_plan_to_string(plan);
        writeln!(explain_result, "{}{}", " ".repeat(level * 2), plan_string).unwrap();
        for child in plan.children() {
            Self::logical_plan_tree_internal(child, level + 1, explain_result);
        }
    }

    pub fn logical_plan_tree(plan: &LogicalOperator) -> String {
        let mut result = String::new();
        Self::logical_plan_tree_internal(plan, 0, &mut result);
        result.trim_end().to_string()
    }

    fn physical_plan_to_string(plan: &PhysicalOperator) -> String {
        match plan {
            PhysicalOperator::PhysicalCreateTable(_) => "PhysicalCreateTable".to_string(),
            PhysicalOperator::PhysicalDummyScan(_) => "PhysicalDummyScan".to_string(),
            PhysicalOperator::PhysicalInsert(_) => "PhysicalInsert".to_string(),
            PhysicalOperator::PhysicalExpressionScan(_) => "PhysicalExpressionScan".to_string(),
            PhysicalOperator::PhysicalTableScan(_) => "PhysicalTableScan".to_string(),
            PhysicalOperator::PhysicalProjection(_) => "PhysicalProjection".to_string(),
            PhysicalOperator::PhysicalColumnDataScan(_) => "PhysicalColumnDataScan".to_string(),
            PhysicalOperator::PhysicalFilter(_) => "PhysicalFilter".to_string(),
            PhysicalOperator::PhysicalLimit(_) => "PhysicalLimit".to_string(),
        }
    }

    fn physical_plan_tree_internal(
        plan: &PhysicalOperator,
        level: usize,
        explain_result: &mut dyn Write,
    ) {
        let plan_string = Self::physical_plan_to_string(plan);
        writeln!(explain_result, "{}{}", " ".repeat(level * 2), plan_string).unwrap();
        for child in plan.children() {
            Self::physical_plan_tree_internal(child, level + 1, explain_result);
        }
    }

    pub fn physical_plan_tree(plan: &PhysicalOperator) -> String {
        let mut result = String::new();
        Self::physical_plan_tree_internal(plan, 0, &mut result);
        result.trim_end().to_string()
    }
}
