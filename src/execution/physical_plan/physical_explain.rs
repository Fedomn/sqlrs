use std::sync::Arc;

use arrow::array::StringArray;
use arrow::record_batch::RecordBatch;

use super::{PhysicalColumnDataScan, PhysicalOperator};
use crate::execution::{PhysicalPlanGenerator, SchemaUtil};
use crate::planner_v2::LogicalExplain;
use crate::util::tree_render::TreeRender;

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_explain(&self, op: LogicalExplain) -> PhysicalOperator {
        let types = op.base.types.clone();
        let logical_child = op.base.children[0].clone();
        // optimized logical plan explain string
        let logical_plan_opt_string = TreeRender::logical_plan_tree(&logical_child);

        let physical_child = self.create_plan_internal(logical_child);
        // physical plan explain string
        let physical_plan_string = TreeRender::physical_plan_tree(&physical_child);

        let base = self.create_physical_operator_base(op.base);

        let schema = SchemaUtil::new_schema_ref(&["type".to_string(), "plan".to_string()], &types);
        let types_column = Arc::new(StringArray::from(vec![
            "logical_plan".to_string(),
            "logical_plan_opt".to_string(),
            "physical_plan".to_string(),
        ]));
        let plans_column = Arc::new(StringArray::from(vec![
            op.logical_plan,
            logical_plan_opt_string,
            physical_plan_string,
        ]));
        let collection = RecordBatch::try_new(schema, vec![types_column, plans_column]).unwrap();
        PhysicalOperator::PhysicalColumnDataScan(PhysicalColumnDataScan::new(
            base,
            vec![collection],
        ))
    }
}
