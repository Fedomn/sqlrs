use std::collections::HashMap;

use itertools::Itertools;
use sqlparser::ast::{
    BinaryOperator, ColumnDef, Expr, FunctionArg, FunctionArgExpr, Ident, ObjectName, Query,
    Select, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins, Value,
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
        let name = column_def.name.value.clone().to_lowercase();
        let ty = column_def.data_type.clone().try_into()?;
        Ok(ColumnDefinition::new(name, ty))
    }

    pub fn resolve_expr_idents(
        idents: &[sqlparser::ast::Ident],
    ) -> Result<(Option<String>, Option<String>, String), BindError> {
        let idents = idents
            .iter()
            .map(|ident| ident.value.to_lowercase())
            .collect_vec();

        let (schema_name, table_name, column_name) = match idents.as_slice() {
            [column] => (None, None, column.clone()),
            [table, column] => (None, Some(table.clone()), column.clone()),
            [schema, table, column] => (Some(schema.clone()), Some(table.clone()), column.clone()),
            _ => return Err(BindError::UnsupportedExpr(format!("{:?}", idents))),
        };
        Ok((schema_name, table_name, column_name))
    }

    pub fn resolve_expr_to_string(e: &Expr) -> Result<String, BindError> {
        match e {
            Expr::Value(v) => match v {
                Value::SingleQuotedString(s) => Ok(s.clone()),
                Value::DoubleQuotedString(s) => Ok(s.clone()),
                _ => Err(BindError::Internal(format!(
                    "excepted string type, but got: {}",
                    v
                ))),
            },
            _ => Err(BindError::Internal(format!(
                "excepted value expr, but got: {}",
                e
            ))),
        }
    }

    pub fn resolve_expr_to_bool(e: &Expr) -> Result<bool, BindError> {
        match e {
            Expr::Value(v) => match v {
                Value::Boolean(b) => Ok(*b),
                _ => Err(BindError::Internal(format!(
                    "excepted bool type, but got: {}",
                    v
                ))),
            },
            _ => Err(BindError::Internal(format!(
                "excepted value expr, but got: {}",
                e
            ))),
        }
    }

    pub fn resolve_func_arg_expr_to_string(arg: &FunctionArgExpr) -> Result<String, BindError> {
        if let FunctionArgExpr::Expr(e) = arg {
            return SqlparserResolver::resolve_expr_to_string(e);
        }
        Err(BindError::Internal(format!(
            "expected string arg, but got {}",
            arg
        )))
    }

    pub fn resolve_func_arg_expr_to_bool(arg: &FunctionArgExpr) -> Result<bool, BindError> {
        if let FunctionArgExpr::Expr(e) = arg {
            return SqlparserResolver::resolve_expr_to_bool(e);
        }
        Err(BindError::Internal(format!(
            "expected bool arg, but got {}",
            arg
        )))
    }
}

#[derive(Default)]
pub struct SqlparserSelectBuilder {
    projection: Vec<SelectItem>,
    from: Vec<TableWithJoins>,
    selection: Option<Expr>,
}

impl SqlparserSelectBuilder {
    pub fn projection(mut self, projection: Vec<SelectItem>) -> Self {
        self.projection = projection;
        self
    }

    pub fn projection_cols(mut self, cols: Vec<&str>) -> Self {
        self.projection = cols
            .into_iter()
            .map(|col| SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(col))))
            .collect();
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

    pub fn from_table_function(mut self, table_function_name: &str) -> Self {
        let relation = TableFactor::Table {
            name: ObjectName(vec![Ident::new(table_function_name)]),
            alias: None,
            args: Some(vec![]),
            with_hints: vec![],
        };
        let table = TableWithJoins {
            relation,
            joins: vec![],
        };
        self.from = vec![table];
        self
    }

    pub fn selection_col_eq_string(mut self, col_name: &str, eq_str: &str) -> Self {
        let selection = Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new(col_name))),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::SingleQuotedString(eq_str.to_string()))),
        };
        self.selection = Some(selection);
        self
    }

    pub fn build(self) -> Select {
        Select {
            distinct: false,
            top: None,
            projection: self.projection,
            into: None,
            from: self.from,
            lateral_views: vec![],
            selection: self.selection,
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

pub struct SqlparserTableFactorBuilder;

impl SqlparserTableFactorBuilder {
    pub fn build_table_func(
        func_name: &str,
        alias: String,
        unamed_arges: Vec<String>,
        uamed_args: HashMap<String, String>,
    ) -> TableFactor {
        let unamed_arges = unamed_arges
            .into_iter()
            .map(|arg| {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(arg),
                )))
            })
            .collect::<Vec<_>>();
        let uamed_args = uamed_args
            .into_iter()
            .map(|(k, v)| FunctionArg::Named {
                name: Ident::new(k),
                arg: FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(v))),
            })
            .collect::<Vec<_>>();
        TableFactor::Table {
            name: ObjectName(vec![Ident::new(func_name)]),
            alias: Some(TableAlias {
                name: Ident::new(alias),
                columns: vec![],
            }),
            args: Some([unamed_arges, uamed_args].concat()),
            with_hints: vec![],
        }
    }
}
