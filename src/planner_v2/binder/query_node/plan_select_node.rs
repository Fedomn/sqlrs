use super::BoundSelectNode;
use crate::planner_v2::BoundTableRef::{BoundBaseTableRef, BoundExpressionListRef};
use crate::planner_v2::{
    BindError, Binder, BoundCastExpression, BoundStatement, LogicalOperator, LogicalOperatorBase,
    LogicalProjection,
};
use crate::types_v2::LogicalType;

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
                    if LogicalType::can_implicit_cast(source_type, target_type) {
                        let alias = node.base.expressioins[idx].alias();
                        node.base.expressioins[idx] = BoundCastExpression::add_cast_to_type(
                            node.base.expressioins[idx].clone(),
                            target_type.clone(),
                            alias,
                            false,
                        );
                        node.base.types[idx] = target_type.clone();
                    } else {
                        return Err(BindError::Internal(format!(
                            "cannot cast {:?} to {:?}",
                            source_type, target_type
                        )));
                    }
                }
            }
            Ok(())
        } else {
            // found a non-projection operator, push a new projection containing the casts
            todo!();
        }
    }
}