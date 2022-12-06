mod bind_base_table_ref;
mod bind_expression_list_ref;
mod plan_base_table_ref;
mod plan_expression_list_ref;
pub use bind_base_table_ref::*;
pub use bind_expression_list_ref::*;
pub use plan_base_table_ref::*;
pub use plan_expression_list_ref::*;

use super::{BindError, Binder};

#[derive(Debug)]
pub enum BoundTableRef {
    BoundExpressionListRef(BoundExpressionListRef),
    BoundBaseTableRef(Box<BoundBaseTableRef>),
}

impl Binder {
    pub fn bind_table_ref(
        &mut self,
        table_refs: &[sqlparser::ast::TableWithJoins],
    ) -> Result<BoundTableRef, BindError> {
        let first_table = table_refs[0].clone();
        match first_table.relation.clone() {
            sqlparser::ast::TableFactor::Table { .. } => {
                self.bind_base_table_ref(first_table.relation)
            }
            other => Err(BindError::Internal(format!(
                "unexpected table type: {}",
                other
            ))),
        }
    }
}
