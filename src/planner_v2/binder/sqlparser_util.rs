use sqlparser::ast::{
    ColumnDef, Ident, ObjectName, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins,
    WildcardAdditionalOptions,
};

use super::BindError;
use crate::catalog_v2::{ColumnDefinition, DEFAULT_SCHEMA};

pub struct SqlparserResolver;

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

#[derive(Default)]
pub struct SqlparserSelectBuilder {
    projection: Vec<SelectItem>,
    from: Vec<TableWithJoins>,
}

impl SqlparserSelectBuilder {
    pub fn projection(mut self, projection: Vec<SelectItem>) -> Self {
        self.projection = projection;
        self
    }

    pub fn projection_wildcard(mut self) -> Self {
        self.projection = vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())];
        self
    }

    pub fn from(mut self, from: Vec<TableWithJoins>) -> Self {
        self.from = from;
        self
    }

    pub fn from_table(mut self, table_name: String) -> Self {
        let relation = TableFactor::Table {
            name: ObjectName(vec![Ident::new(table_name)]),
            alias: None,
            args: None,
            with_hints: vec![],
        };
        let table = TableWithJoins {
            relation,
            joins: vec![],
        };
        self.from = vec![table];
        self
    }

    pub fn build(self) -> sqlparser::ast::Select {
        sqlparser::ast::Select {
            distinct: false,
            top: None,
            projection: self.projection,
            into: None,
            from: self.from,
            lateral_views: vec![],
            selection: None,
            group_by: vec![],
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            qualify: None,
        }
    }
}

pub struct SqlparserQueryBuilder {
    body: Box<SetExpr>,
}

impl SqlparserQueryBuilder {
    pub fn new_from_select(select: Select) -> Self {
        Self {
            body: Box::new(SetExpr::Select(Box::new(select))),
        }
    }

    pub fn build(self) -> Query {
        Query {
            with: None,
            body: self.body,
            order_by: vec![],
            limit: None,
            offset: None,
            fetch: None,
            lock: None,
        }
    }
}
