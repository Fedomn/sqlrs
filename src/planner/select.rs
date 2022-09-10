use std::sync::Arc;

use super::util::find_aggregate_exprs;
use super::*;
use crate::binder::{BoundSelect, BoundTableRef};
use crate::optimizer::*;

impl Planner {
    pub fn plan_select(&self, stmt: BoundSelect) -> Result<PlanRef, LogicalPlanError> {
        let mut plan: PlanRef;

        if let Some(table_ref) = stmt.from_table {
            // plan table_ref into LogicalTableScan or LogicalJoin
            plan = self.plan_table_ref(&table_ref)?;
        } else {
            todo!("need logical values")
        }

        if let Some(expr) = stmt.where_clause {
            plan = Arc::new(LogicalFilter::new(expr, plan));
        }

        let agg = find_aggregate_exprs(stmt.select_list.as_slice());

        if !agg.is_empty() || !stmt.group_by.is_empty() {
            plan = Arc::new(LogicalAgg::new(agg, stmt.group_by, plan));
        }

        if !stmt.select_list.is_empty() {
            plan = Arc::new(LogicalProject::new(stmt.select_list, plan));
        }

        if !stmt.order_by.is_empty() {
            plan = Arc::new(LogicalOrder::new(stmt.order_by, plan));
        }

        // the last step is to limit data size
        if stmt.limit.is_some() || stmt.offset.is_some() {
            plan = Arc::new(LogicalLimit::new(stmt.limit, stmt.offset, plan));
        }

        Ok(plan)
    }

    fn plan_table_ref(&self, table_ref: &BoundTableRef) -> Result<PlanRef, LogicalPlanError> {
        match table_ref {
            BoundTableRef::Table(table_catalog) => Ok(Arc::new(LogicalTableScan::new(
                table_catalog.id.clone(),
                table_catalog.get_all_columns(),
                None,
            ))),
            BoundTableRef::Join(join) => {
                // same as Binder::bind_table_with_joins
                // use left-deep to construct multiple joins
                // join ordering refer to: https://www.cockroachlabs.com/blog/join-ordering-pt1/
                let left = self.plan_table_ref(&join.left)?;
                let right = self.plan_table_ref(&join.right)?;
                let join = LogicalJoin::new(
                    left,
                    right,
                    join.join_type.clone(),
                    join.join_condition.clone(),
                );
                Ok(Arc::new(join))
            }
        }
    }
}
