use std::collections::HashMap;

use sqlparser::ast::Statement;

use super::BoundStatement;
use crate::catalog_v2::Catalog;
use crate::planner_v2::{
    BindError, Binder, BoundTableRef, LogicalInsert, LogicalOperator, LogicalOperatorBase,
    SqlparserResolver, INVALID_INDEX,
};
use crate::types_v2::LogicalType;

impl Binder {
    pub fn bind_insert(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let (schema_name, table_name) =
                    SqlparserResolver::object_name_to_schema_table(table_name)?;
                let table = Catalog::get_table(
                    self.clone_client_context(),
                    schema_name,
                    table_name.clone(),
                )?;

                // insert column mapped to table column type
                let mut expected_types = vec![];
                // insert column mapped to table column index
                let mut named_column_indices = vec![];
                // The insertion map ([table_index -> index in result, or DConstants::INVALID_INDEX
                // if not specified])
                let mut column_index_list = vec![];
                if columns.is_empty() {
                    for (idx, col) in table.columns.iter().enumerate() {
                        named_column_indices.push(idx);
                        column_index_list.push(idx);
                        expected_types.push(col.ty.clone());
                    }
                } else {
                    // insertion statement specifies column list
                    // column_name to insert columns index
                    let mut column_name_2_insert_idx_map = HashMap::new();
                    for (idx, col) in columns.iter().enumerate() {
                        column_name_2_insert_idx_map.insert(col.value.clone(), idx);
                        let column_index = match table.name_map.get(col.value.as_str()) {
                            Some(e) => e,
                            None => {
                                return Err(BindError::Internal(format!(
                                    "column {} not found in table {}",
                                    col.value, table_name
                                )))
                            }
                        };
                        expected_types.push(table.columns[*column_index].ty.clone());
                        named_column_indices.push(*column_index);
                    }
                    for col in table.columns.iter() {
                        let insert_column_index =
                            match column_name_2_insert_idx_map.get(col.name.as_str()) {
                                Some(i) => *i,
                                None => INVALID_INDEX,
                            };
                        column_index_list.push(insert_column_index);
                    }
                }

                let select_node = self.bind_select_node(source)?;
                let expected_columns_cnt = named_column_indices.len();
                if let BoundTableRef::BoundExpressionListRef(table_ref) = &select_node.from_table {
                    // CheckInsertColumnCountMismatch
                    let insert_columns_cnt = table_ref.values.first().unwrap().len();
                    if expected_columns_cnt != insert_columns_cnt {
                        return Err(BindError::Internal(format!(
                            "insert column count mismatch, expected: {}, actual: {}",
                            expected_columns_cnt, insert_columns_cnt
                        )));
                    }
                };

                // TODO: cast types

                let select_node = self.create_plan_for_select_node(select_node)?;
                let plan = select_node.plan;
                let root = LogicalInsert::new(
                    LogicalOperatorBase::new(vec![plan], vec![], vec![]),
                    column_index_list,
                    expected_types,
                    table,
                );
                Ok(BoundStatement::new(
                    LogicalOperator::LogicalInsert(root),
                    vec![LogicalType::Varchar],
                    vec!["success".to_string()],
                ))
            }
            _ => Err(BindError::UnsupportedStmt(format!("{:?}", stmt))),
        }
    }
}
