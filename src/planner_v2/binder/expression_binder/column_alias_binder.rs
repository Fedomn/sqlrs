use std::collections::HashMap;

use derive_new::new;

#[derive(new)]
pub struct ColumnAliasData {
    pub(crate) original_select_items: Vec<sqlparser::ast::Expr>,
    pub(crate) alias_map: HashMap<String, usize>,
}
