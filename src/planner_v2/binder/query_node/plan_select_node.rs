use super::BoundSelectNode;
use crate::planner_v2::BoundTableRef::{
    BoundBaseTableRef, BoundDummyTableRef, BoundExpressionListRef, BoundTableFunction,
};
use crate::planner_v2::{
    BindError, Binder, BoundCastExpression, BoundStatement, LogicalFilter, LogicalOperator,
    LogicalOperatorBase, LogicalProjection,
};
use crate::types_v2::LogicalType;

impl Binder {
    pub fn create_plan_for_select_node(
        &mut self,
        node: BoundSelectNode,
    ) -> Result<BoundStatement, BindError> {
        let mut root = match node.from_table {
            BoundExpressionListRef(bound_ref) => {
                self.create_plan_for_expression_list_ref(bound_ref)?
            }
            BoundBaseTableRef(bound_ref) => self.create_plan_for_base_tabel_ref(*bound_ref)?,
            BoundDummyTableRef(bound_ref) => self.create_plan_for_dummy_table_ref(bound_ref)?,
            BoundTableFunction(bound_func) => self.create_plan_for_table_function(*bound_func)?,
        };

        if let Some(where_clause) = node.where_clause {
            root = LogicalOperator::LogicalFilter(LogicalFilter::new(LogicalOperatorBase::new(
                vec![root],
                vec![where_clause],
                vec![],
            )));
        }

        let root = LogicalOperator::LogicalProjection(LogicalProjection::new(
            LogicalOperatorBase::new(vec![root], node.select_list, node.types.clone()),
            node.projection_index,
        ));

        let result_modifiers = node.modifiers;
        let root = self.plan_for_result_modifiers(result_modifiers, root)?;

        Ok(BoundStatement::new(root, node.types, node.names))
    }

    pub fn cast_logical_operator_to_types(
        &mut self,
        source_types: &[LogicalType],
        target_types: &[LogicalType],
        op: &mut LogicalOperator,
    ) -> Result<(), BindError> {
        assert!(source_types.len() == target_types.len());
        if source_types == target_types {
            // source and target types are equal: don't need to cast
            return Ok(());
        }
        if let LogicalOperator::LogicalProjection(node) = op {
            // "node" is a projection; we can just do the casts in there
            assert!(node.base.expressioins.len() == source_types.len());
            for (idx, (source_type, target_type)) in
                source_types.iter().zip(target_types.iter()).enumerate()
            {
                if source_type != target_type {
                    // differing types, have to add a cast but may be lossy
                    node.base.expressioins[idx] = BoundCastExpression::try_add_cast_to_type(
                        node.base.expressioins[idx].clone(),
                        target_type.clone(),
                        false,
                    )?;
                    node.base.types[idx] = target_type.clone();
                }
            }
            Ok(())
        } else {
            // found a non-projection operator, push a new projection containing the casts
            todo!();
        }
    }
}
