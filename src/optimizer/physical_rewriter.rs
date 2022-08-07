use std::sync::Arc;

use super::plan_rewriter::PlanRewriter;
use super::{
    LogicalAgg, LogicalFilter, LogicalProject, LogicalTableScan, PhysicalHashAgg, PhysicalLimit,
    PhysicalOrder, PhysicalSimpleAgg, PhysicalTableScan, PlanRef, PlanTreeNode,
};
use crate::optimizer::{PhysicalFilter, PhysicalProject};

pub struct PhysicalRewriter {}

impl PlanRewriter for PhysicalRewriter {
    fn rewrite_logical_table_scan(&mut self, plan: &LogicalTableScan) -> PlanRef {
        Arc::new(PhysicalTableScan::new(plan.clone()))
    }

    fn rewrite_logical_project(&mut self, plan: &LogicalProject) -> PlanRef {
        let child = self.rewrite(plan.children().first().unwrap().clone());
        let logical = plan.clone_with_children([child].to_vec());
        Arc::new(PhysicalProject::new(
            logical.as_logical_project().unwrap().clone(),
        ))
    }

    fn rewrite_logical_filter(&mut self, plan: &LogicalFilter) -> PlanRef {
        let child = self.rewrite(plan.children().first().unwrap().clone());
        let logical = plan.clone_with_children([child].to_vec());
        Arc::new(PhysicalFilter::new(
            logical.as_logical_filter().unwrap().clone(),
        ))
    }

    fn rewrite_logical_agg(&mut self, plan: &LogicalAgg) -> PlanRef {
        let child = self.rewrite(plan.children().first().unwrap().clone());
        let logical = plan.clone_with_children([child].to_vec());
        let logical_plan = logical.as_logical_agg().unwrap().clone();
        if logical_plan.group_by().is_empty() {
            Arc::new(PhysicalSimpleAgg::new(
                logical.as_logical_agg().unwrap().clone(),
            ))
        } else {
            Arc::new(PhysicalHashAgg::new(
                logical.as_logical_agg().unwrap().clone(),
            ))
        }
    }

    fn rewrite_logical_limit(&mut self, plan: &super::LogicalLimit) -> PlanRef {
        let child = self.rewrite(plan.children().first().unwrap().clone());
        let logical = plan.clone_with_children([child].to_vec());
        Arc::new(PhysicalLimit::new(
            logical.as_logical_limit().unwrap().clone(),
        ))
    }

    fn rewrite_logical_order(&mut self, plan: &super::LogicalOrder) -> PlanRef {
        let child = self.rewrite(plan.children().first().unwrap().clone());
        let logical = plan.clone_with_children([child].to_vec());
        Arc::new(PhysicalOrder::new(
            logical.as_logical_order().unwrap().clone(),
        ))
    }
}

#[cfg(test)]
mod physical_rewriter_test {
    use arrow::datatypes::DataType::{self, Int32};
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::binder::{BoundBinaryOp, BoundColumnRef, BoundExpr};
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::optimizer::PlanNodeType;
    use crate::types::ScalarValue;

    fn build_test_column(column_name: String) -> ColumnCatalog {
        ColumnCatalog {
            table_id: "t".to_string(),
            column_id: column_name.clone(),
            desc: ColumnDesc {
                name: column_name,
                data_type: Int32,
            },
        }
    }

    #[test]
    fn test_physical_rewriter_works() {
        let mut rewriter = PhysicalRewriter {};
        let table_id = "t".to_string();
        let columns = [
            build_test_column("c1".to_string()),
            build_test_column("c2".to_string()),
        ]
        .to_vec();
        let mut plan: PlanRef;
        plan = Arc::new(LogicalTableScan::new(table_id, columns));
        let filter_expr = BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::Eq,
            left: Box::new(BoundExpr::ColumnRef(BoundColumnRef {
                column_catalog: build_test_column("c2".to_string()),
            })),
            right: Box::new(BoundExpr::Constant(ScalarValue::Int32(Some(2)))),
            return_type: Some(DataType::Boolean),
        });
        plan = Arc::new(LogicalFilter::new(filter_expr, plan));
        let project_expr = BoundExpr::ColumnRef(BoundColumnRef {
            column_catalog: build_test_column("c1".to_string()),
        });
        plan = Arc::new(LogicalProject::new([project_expr].to_vec(), plan));

        let mut physical_plan = rewriter.rewrite(plan);

        assert_eq!(physical_plan.node_type(), PlanNodeType::PhysicalProject);
        physical_plan = physical_plan.children().first().unwrap().clone();
        assert_eq!(physical_plan.node_type(), PlanNodeType::PhysicalFilter);
        physical_plan = physical_plan.children().first().unwrap().clone();
        assert_eq!(physical_plan.node_type(), PlanNodeType::PhysicalTableScan);
    }
}
