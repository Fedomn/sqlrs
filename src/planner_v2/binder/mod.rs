mod bind_context;
mod binding;
mod errors;
mod expression;
mod expression_binder;
mod query_node;
mod sqlparser_util;
mod statement;
mod tableref;

use std::sync::Arc;

pub use bind_context::*;
pub use binding::*;
pub use errors::*;
pub use expression::*;
pub use expression_binder::*;
pub use query_node::*;
pub use sqlparser_util::*;
pub use statement::*;
pub use tableref::*;

use crate::main_entry::ClientContext;

#[derive(Clone)]
pub struct Binder {
    client_context: Arc<ClientContext>,
    bind_context: BindContext,
    /// The count of bound_tables
    bound_tables: usize,
    #[allow(dead_code)]
    parent: Option<Arc<Binder>>,
}

impl Binder {
    pub fn new(client_context: Arc<ClientContext>) -> Self {
        Self {
            client_context,
            bind_context: BindContext::new(),
            bound_tables: 0,
            parent: None,
        }
    }

    pub fn new_with_parent(client_context: Arc<ClientContext>, parent: Arc<Binder>) -> Self {
        Self {
            client_context,
            bind_context: BindContext::new(),
            bound_tables: 0,
            parent: Some(parent),
        }
    }

    pub fn clone_client_context(&self) -> Arc<ClientContext> {
        self.client_context.clone()
    }

    pub fn generate_table_index(&mut self) -> usize {
        self.bound_tables += 1;
        self.bound_tables
    }

    pub fn has_match_binding(&mut self, table_name: &str, column_name: &str) -> bool {
        let binding = self.bind_context.get_binding(table_name);
        if binding.is_none() {
            return false;
        }
        let binding = binding.unwrap();
        binding.has_match_binding(column_name)
    }
}
