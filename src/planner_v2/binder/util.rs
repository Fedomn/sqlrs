use sqlparser::ast::{ColumnDef, ObjectName};

use super::BindError;
use crate::catalog_v2::{ColumnDefinition, DEFAULT_SCHEMA};

pub struct SqlparserResolver {}

impl SqlparserResolver {
    /// Resolve object_name which is a name of a table, view, custom type, etc., possibly
    /// multi-part, i.e. db.schema.obj
    pub fn object_name_to_schema_table(
        object_name: &ObjectName,
    ) -> Result<(String, String), BindError> {
        let (schema, table) = match object_name.0.as_slice() {
            [table] => (DEFAULT_SCHEMA.to_string(), table.value.clone()),
            [schema, table] => (schema.value.clone(), table.value.clone()),
            _ => return Err(BindError::SqlParserUnsupportedStmt(object_name.to_string())),
        };
        Ok((schema, table))
    }

    pub fn column_def_to_column_definition(
        column_def: &ColumnDef,
    ) -> Result<ColumnDefinition, BindError> {
        let name = column_def.name.value.clone();
        let ty = column_def.data_type.clone().try_into()?;
        Ok(ColumnDefinition::new(name, ty))
    }
}
