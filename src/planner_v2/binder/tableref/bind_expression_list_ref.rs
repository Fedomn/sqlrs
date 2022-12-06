use derive_new::new;
use sqlparser::ast::Values;

use crate::planner_v2::{BindError, Binder, BoundExpression, ExpressionBinder};
use crate::types_v2::LogicalType;

pub static VALUES_LIST_ALIAS: &str = "valueslist";

/// Represents a TableReference to a base table in the schema
#[derive(new, Debug)]
pub struct BoundExpressionListRef {
    /// The bound VALUES list
    pub(crate) values: Vec<Vec<BoundExpression>>,
    /// The generated names of the values list
    pub(crate) names: Vec<String>,
    /// The types of the values list
    pub(crate) types: Vec<LogicalType>,
    /// The index in the bind context
    pub(crate) bind_index: usize,
}

impl Binder {
    pub fn bind_expression_list_ref(
        &mut self,
        values: &Values,
    ) -> Result<BoundExpressionListRef, BindError> {
        let mut bound_expr_list = vec![];
        let mut names = vec![];
        let mut types = vec![];
        let mut finish_name = false;

        let mut expr_binder = ExpressionBinder::new(self);

        for val_expr_list in values.0.iter() {
            let mut bound_expr_row = vec![];
            for (idx, expr) in val_expr_list.iter().enumerate() {
                let bound_expr = expr_binder.bind_expression(expr, &mut vec![], &mut vec![])?;
                if !finish_name {
                    names.push(format!("col{}", idx));
                    types.push(bound_expr.return_type());
                }
                bound_expr_row.push(bound_expr);
            }
            bound_expr_list.push(bound_expr_row);
            finish_name = true;
        }
        let table_index = self.generate_table_index();
        self.bind_context.add_generic_binding(
            VALUES_LIST_ALIAS.to_string(),
            table_index,
            types.clone(),
            names.clone(),
        );
        let bound_ref = BoundExpressionListRef::new(bound_expr_list, names, types, table_index);
        Ok(bound_ref)
    }
}
