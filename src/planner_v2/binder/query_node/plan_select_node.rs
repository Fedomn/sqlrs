use super::BoundSelectNode;
use crate::planner_v2::BoundTableRef::{BoundBaseTableRef, BoundExpressionListRef};
use crate::planner_v2::{
    BindError, Binder, BoundStatement, LogicalOperator, LogicalOperatorBase, LogicalProjection,
};

impl Binder {
    pub fn create_plan_for_select_node(
        &mut self,
        node: BoundSelectNode,
    ) -> Result<BoundStatement, BindError> {
        let root = match node.from_table {
            BoundExpressionListRef(bound_ref) => {
                self.create_plan_for_expression_list_ref(bound_ref)?
            }
            BoundBaseTableRef(bound_ref) => self.create_plan_for_base_tabel_ref(*bound_ref)?,
        };

        let root = LogicalOperator::LogicalProjection(LogicalProjection::new(
            LogicalOperatorBase::new(vec![root], node.select_list, node.types.clone()),
            node.projection_index,
        ));

        Ok(BoundStatement::new(root, node.types, node.names))
    }
}
