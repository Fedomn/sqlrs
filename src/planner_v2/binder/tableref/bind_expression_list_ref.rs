use derive_new::new;
use sqlparser::ast::Values;

use crate::planner_v2::{
    BindError, Binder, BoundCastExpression, BoundExpression, ExpressionBinder,
};
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
        // ensure all values lists are the same length
        let mut values_cnt = 0;
        for val_expr_list in values.rows.iter() {
            if values_cnt == 0 {
                values_cnt = val_expr_list.len();
            } else if values_cnt != val_expr_list.len() {
                return Err(BindError::Internal(
                    "VALUES lists must all be the same length".to_string(),
                ));
            }
        }

        let mut bound_expr_list = vec![];
        let mut names = vec!["".to_string(); values_cnt];
        let mut types = vec![LogicalType::Invalid; values_cnt];

        let mut expr_binder = ExpressionBinder::new(self);

        for val_expr_list in values.rows.iter() {
            let mut bound_expr_row = vec![];
            for (idx, expr) in val_expr_list.iter().enumerate() {
                let bound_expr = expr_binder.bind_expression(expr, &mut vec![], &mut vec![])?;
                names[idx] = format!("col{}", idx);
                if types[idx] == LogicalType::Invalid {
                    types[idx] = bound_expr.return_type().clone();
                }
                // use values max type as the column type
                types[idx] = LogicalType::max_logical_type(&types[idx], &bound_expr.return_type())?;
                bound_expr_row.push(bound_expr);
            }
            bound_expr_list.push(bound_expr_row);
        }
        // insert values contains SqlNull, the expr should be cast to the max logical type
        for exprs in bound_expr_list.iter_mut() {
            for (idx, bound_expr) in exprs.iter_mut().enumerate() {
                if bound_expr.return_type() != types[idx] {
                    *bound_expr = BoundCastExpression::try_add_cast_to_type(
                        bound_expr.clone(),
                        types[idx].clone(),
                        false,
                    )?
                }
            }
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
