use std::collections::HashMap;

use derive_new::new;

use super::{BindError, Binding, BoundColumnRefExpression};
use crate::catalog_v2::CatalogEntry;
use crate::types_v2::LogicalType;

/// The BindContext object keeps track of all the tables and columns
/// that are encountered during the binding process.
#[derive(new, Debug, Clone)]
pub struct BindContext {
    /// table name -> table binding
    #[new(default)]
    pub(crate) bindings: HashMap<String, Binding>,
    #[new(default)]
    pub(crate) binding_list: Vec<Binding>,
}

impl BindContext {
    pub fn add_binding(
        &mut self,
        alias: String,
        index: usize,
        types: Vec<LogicalType>,
        names: Vec<String>,
        catalog_entry: Option<CatalogEntry>,
    ) {
        let name_map = names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();
        let mut binding = Binding::new(alias.clone(), index, types, names, name_map);
        binding.catalog_entry = catalog_entry;
        self.bindings.insert(alias, binding.clone());
        self.binding_list.push(binding);
    }

    pub fn add_generic_binding(
        &mut self,
        alias: String,
        index: usize,
        types: Vec<LogicalType>,
        names: Vec<String>,
    ) {
        self.add_binding(alias, index, types, names, None);
    }

    pub fn add_base_table(
        &mut self,
        alias: String,
        index: usize,
        types: Vec<LogicalType>,
        names: Vec<String>,
        catalog_entry: CatalogEntry,
    ) {
        self.add_binding(alias, index, types, names, Some(catalog_entry));
    }

    pub fn add_table_function(
        &mut self,
        alias: String,
        index: usize,
        types: Vec<LogicalType>,
        names: Vec<String>,
        catalog_entry: CatalogEntry,
    ) {
        self.add_binding(alias, index, types, names, Some(catalog_entry));
    }

    pub fn get_binding(&self, table_name: &str) -> Option<Binding> {
        self.bindings.get(table_name).cloned()
    }

    pub fn get_matching_binding(&self, column_name: &str) -> Result<String, BindError> {
        let mut mathing_table_name = None;
        for binding in self.binding_list.iter() {
            if binding.has_match_binding(column_name) {
                if mathing_table_name.is_some() {
                    return Err(BindError::Internal(format!(
                        "Ambiguous column name {}",
                        column_name
                    )));
                }
                mathing_table_name = Some(binding.alias.clone());
            }
        }
        if let Some(table_name) = mathing_table_name {
            Ok(table_name)
        } else {
            Err(BindError::Internal(format!(
                "Column {} not found in any table",
                column_name
            )))
        }
    }

    pub fn bind_column(
        &mut self,
        table_name: &str,
        column_name: &str,
    ) -> Result<BoundColumnRefExpression, BindError> {
        if let Some(table_binding) = self.get_binding(table_name) {
            table_binding.bind_column(column_name, 0)
        } else {
            Err(BindError::Internal(format!(
                "Table {} not found in context",
                table_name
            )))
        }
    }

    pub fn generate_all_column_expressions(
        &mut self,
        table_name: Option<String>,
    ) -> Result<Vec<sqlparser::ast::SelectItem>, BindError> {
        use sqlparser::ast;
        let select_items = if let Some(table_name) = table_name {
            if let Some(binding) = self.get_binding(table_name.as_str()) {
                binding
                    .names
                    .iter()
                    .map(|col_name| {
                        ast::SelectItem::UnnamedExpr(ast::Expr::CompoundIdentifier(vec![
                            ast::Ident::new(binding.alias.clone()),
                            ast::Ident::new(col_name.clone()),
                        ]))
                    })
                    .collect::<Vec<_>>()
            } else {
                return Err(BindError::Internal(format!(
                    "Table {} not found in context",
                    table_name
                )));
            }
        } else {
            self.binding_list
                .iter()
                .flat_map(|binding| {
                    binding
                        .names
                        .iter()
                        .map(|col_name| {
                            ast::SelectItem::UnnamedExpr(ast::Expr::CompoundIdentifier(vec![
                                ast::Ident::new(binding.alias.clone()),
                                ast::Ident::new(col_name.clone()),
                            ]))
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        Ok(select_items)
    }
}
