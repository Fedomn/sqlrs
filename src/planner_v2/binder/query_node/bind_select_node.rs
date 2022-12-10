use derive_new::new;
use sqlparser::ast::{Ident, Query};

use crate::planner_v2::{
    BindError, Binder, BoundExpression, BoundTableRef, ExpressionBinder, VALUES_LIST_ALIAS,
};
use crate::types_v2::LogicalType;

#[derive(new, Debug)]
pub struct BoundSelectNode {
    /// The names returned by this QueryNode.
    pub(crate) names: Vec<String>,
    /// The types returned by this QueryNode.
    pub(crate) types: Vec<LogicalType>,
    /// The projection list
    pub(crate) select_list: Vec<BoundExpression>,
    /// The FROM clause
    pub(crate) from_table: BoundTableRef,
    /// Index used by the LogicalProjection
    #[new(default)]
    pub(crate) projection_index: usize,
}

impl Binder {
    pub fn bind_select_node(&mut self, select_node: &Query) -> Result<BoundSelectNode, BindError> {
        let projection_index = self.generate_table_index();
        let mut bound_select_node = match &*select_node.body {
            sqlparser::ast::SetExpr::Select(select) => self.bind_select_body(select)?,
            sqlparser::ast::SetExpr::Query(_) => todo!(),
            sqlparser::ast::SetExpr::SetOperation { .. } => todo!(),
            sqlparser::ast::SetExpr::Values(v) => self.bind_values(v)?,
            sqlparser::ast::SetExpr::Insert(_) => todo!(),
            sqlparser::ast::SetExpr::Table(_) => todo!(),
        };
        bound_select_node.projection_index = projection_index;
        Ok(bound_select_node)
    }

    pub fn bind_values(
        &mut self,
        values: &sqlparser::ast::Values,
    ) -> Result<BoundSelectNode, BindError> {
        let bound_expression_list_ref = self.bind_expression_list_ref(values)?;
        let names = bound_expression_list_ref.names.clone();
        let types = bound_expression_list_ref.types.clone();
        let mut expr_binder = ExpressionBinder::new(self);
        let select_list = names
            .iter()
            .map(|n| {
                let idents = vec![
                    Ident::new(VALUES_LIST_ALIAS.to_string()),
                    Ident::new(n.to_string()),
                ];
                expr_binder.bind_column_ref_expr(&idents, &mut vec![], &mut vec![])
            })
            .try_collect::<Vec<_>>()?;

        let bound_table_ref = BoundTableRef::BoundExpressionListRef(bound_expression_list_ref);
        let node = BoundSelectNode::new(names, types, select_list, bound_table_ref);
        Ok(node)
    }

    pub fn bind_select_body(
        &mut self,
        select: &sqlparser::ast::Select,
    ) -> Result<BoundSelectNode, BindError> {
        let from_table = self.bind_table_ref(select.from.as_slice())?;

        let mut result_names = vec![];
        let mut result_types = vec![];
        let select_list = select
            .projection
            .iter()
            .map(|item| self.bind_select_item(item, &mut result_names, &mut result_types))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(BoundSelectNode::new(
            result_names,
            result_types,
            select_list,
            from_table,
        ))
    }

    fn bind_select_item(
        &mut self,
        item: &sqlparser::ast::SelectItem,
        result_names: &mut Vec<String>,
        result_types: &mut Vec<LogicalType>,
    ) -> Result<BoundExpression, BindError> {
        let mut expr_binder = ExpressionBinder::new(self);
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                expr_binder.bind_expression(expr, result_names, result_types)
            }
            sqlparser::ast::SelectItem::ExprWithAlias { .. } => todo!(),
            sqlparser::ast::SelectItem::Wildcard(_) => todo!(),
            sqlparser::ast::SelectItem::QualifiedWildcard(_, _) => todo!(),
        }
    }
}
