use std::sync::Arc;

use super::util::find_aggregate_exprs;
use super::*;
use crate::binder::{BoundSelect, BoundTableRef};
use crate::optimizer::*;

impl Planner {
    pub fn plan_select(&mut self, stmt: BoundSelect) -> Result<PlanRef, LogicalPlanError> {
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

        if stmt.select_distinct {
            // convert distinct to groupby with no aggregations
            plan = Arc::new(LogicalAgg::new(vec![], stmt.select_list.clone(), plan));
        }

        // LogicalOrder should be below LogicalProject in tree due to it could contains column_ref
        if !stmt.order_by.is_empty() {
            plan = Arc::new(LogicalOrder::new(stmt.order_by, plan));
        }

        if !stmt.select_list.is_empty() {
            plan = Arc::new(LogicalProject::new(stmt.select_list, plan));
        }

        // the last step is to limit data size
        if stmt.limit.is_some() || stmt.offset.is_some() {
            plan = Arc::new(LogicalLimit::new(stmt.limit, stmt.offset, plan));
        }

        Ok(plan)
    }

    fn plan_table_ref(&mut self, table_ref: &BoundTableRef) -> Result<PlanRef, LogicalPlanError> {
        match table_ref {
            BoundTableRef::Table(table) => Ok(Arc::new(LogicalTableScan::new(
                table.catalog.id.clone(),
                table.alias.clone(),
                table.catalog.get_all_columns(),
                None,
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
            BoundTableRef::Subquery(subquery) => {
                let subquery = subquery.clone();
                let plan_ref = self.plan_select(*subquery.query)?;
                Ok(plan_ref)
            }
        }
    }
}
