mod agg_func;
mod binary_op;
use std::{fmt, slice};

pub use agg_func::*;
use arrow::datatypes::DataType;
pub use binary_op::*;
use itertools::Itertools;
use sqlparser::ast::{Expr, Ident};

use super::{BindError, Binder};
use crate::catalog::ColumnCatalog;
use crate::types::ScalarValue;

#[derive(Clone, PartialEq, Eq)]
pub enum BoundExpr {
    Constant(ScalarValue),
    ColumnRef(BoundColumnRef),
    /// InputRef represents an index of the RecordBatch, which is resolved in optimizer.
    InputRef(BoundInputRef),
    BinaryOp(BoundBinaryOp),
    TypeCast(BoundTypeCast),
    AggFunc(BoundAggFunc),
}

impl BoundExpr {
    pub fn return_type(&self) -> Option<DataType> {
        match self {
            BoundExpr::Constant(value) => Some(value.data_type()),
            BoundExpr::InputRef(input) => Some(input.return_type.clone()),
            BoundExpr::ColumnRef(column_ref) => {
                Some(column_ref.column_catalog.desc.data_type.clone())
            }
            BoundExpr::BinaryOp(binary_op) => binary_op.return_type.clone(),
            BoundExpr::TypeCast(tc) => Some(tc.cast_type.clone()),
            BoundExpr::AggFunc(agg) => Some(agg.return_type.clone()),
        }
    }

    pub fn contains_column_ref(&self) -> bool {
        match self {
            BoundExpr::Constant(_) => false,
            BoundExpr::InputRef(_) => false,
            BoundExpr::ColumnRef(_) => true,
            BoundExpr::BinaryOp(binary_op) => {
                binary_op.left.contains_column_ref() || binary_op.right.contains_column_ref()
            }
            BoundExpr::TypeCast(tc) => tc.expr.contains_column_ref(),
            BoundExpr::AggFunc(agg) => agg.exprs.iter().any(|arg| arg.contains_column_ref()),
        }
    }

    pub fn get_column_catalog(&self) -> Vec<ColumnCatalog> {
        match self {
            BoundExpr::Constant(_) => vec![],
            BoundExpr::InputRef(_) => vec![],
            BoundExpr::ColumnRef(column_ref) => vec![column_ref.column_catalog.clone()],
            BoundExpr::BinaryOp(binary_op) => binary_op
                .left
                .get_column_catalog()
                .into_iter()
                .chain(binary_op.right.get_column_catalog().into_iter())
                .collect::<Vec<_>>(),
            BoundExpr::TypeCast(tc) => tc.expr.get_column_catalog(),
            BoundExpr::AggFunc(agg) => agg
                .exprs
                .iter()
                .flat_map(|arg| arg.get_column_catalog())
                .collect::<Vec<_>>(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct BoundColumnRef {
    pub column_catalog: ColumnCatalog,
}

#[derive(Clone, PartialEq, Eq)]
pub struct BoundInputRef {
    /// column index in data chunk
    pub index: usize,
    pub return_type: DataType,
}

#[derive(Clone, PartialEq, Eq)]
pub struct BoundTypeCast {
    /// original expression
    pub expr: Box<BoundExpr>,
    pub cast_type: DataType,
}

impl Binder {
    /// bind sqlparser Expr into BoundExpr
    pub fn bind_expr(&mut self, expr: &Expr) -> Result<BoundExpr, BindError> {
        match expr {
            Expr::Identifier(ident) => {
                self.bind_column_ref_from_identifiers(slice::from_ref(ident))
            }
            Expr::CompoundIdentifier(idents) => self.bind_column_ref_from_identifiers(idents),
            Expr::BinaryOp { left, op, right } => self.bind_binary_op(left, op, right),
            Expr::UnaryOp { op: _, expr: _ } => todo!(),
            Expr::Value(v) => Ok(BoundExpr::Constant(v.into())),
            Expr::Function(func) => self.bind_agg_func(func),
            Expr::Nested(expr) => self.bind_expr(expr),
            _ => todo!("unsupported expr {:?}", expr),
        }
    }

    /// bind sqlparser Identifier into BoundExpr
    ///
    /// Identifier types:
    ///  * Identifier(Ident): Identifier e.g. table name or column name
    ///  * CompoundIdentifier(Vec<Ident>): Multi-part identifier, e.g. `table_alias.column` or
    ///    `schema.table.col`
    ///
    /// so, the idents slice could be `[col]`, `[table, col]` or `[schema, table, col]`
    pub fn bind_column_ref_from_identifiers(
        &mut self,
        idents: &[Ident],
    ) -> Result<BoundExpr, BindError> {
        let idents = idents
            .iter()
            .map(|ident| Ident::new(ident.value.to_lowercase()))
            .collect_vec();

        let (_schema_name, table_name, column_name) = match idents.as_slice() {
            [column] => (None, None, &column.value),
            [table, column] => (None, Some(&table.value), &column.value),
            [schema, table, column] => (Some(&schema.value), Some(&table.value), &column.value),
            _ => return Err(BindError::InvalidTableName(idents)),
        };

        if let Some(table) = table_name {
            // handle table.col syntax
            let table_catalog = self.context.tables.get(table).unwrap();
            let column_catalog = table_catalog
                .get_column_by_name(column_name)
                .ok_or_else(|| BindError::InvalidColumn(column_name.clone()))?;
            Ok(BoundExpr::ColumnRef(BoundColumnRef { column_catalog }))
        } else {
            // handle col syntax
            let mut got_column = None;
            for table_catalog in self.context.tables.values() {
                if let Some(column_catalog) = table_catalog.get_column_by_name(column_name) {
                    // ambiguous column check
                    if got_column.is_some() {
                        return Err(BindError::AmbiguousColumn(column_name.clone()));
                    }
                    got_column = Some(column_catalog);
                }
            }
            let column_catalog =
                got_column.ok_or_else(|| BindError::InvalidColumn(column_name.clone()))?;
            Ok(BoundExpr::ColumnRef(BoundColumnRef { column_catalog }))
        }
    }
}

impl fmt::Debug for BoundExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BoundExpr::Constant(value) => write!(f, "{}", value),
            BoundExpr::ColumnRef(column_ref) => write!(f, "{:?}", column_ref),
            BoundExpr::InputRef(input_ref) => write!(f, "{:?}", input_ref),
            BoundExpr::BinaryOp(binary_op) => write!(f, "{:?}", binary_op),
            BoundExpr::TypeCast(type_cast) => write!(f, "{:?}", type_cast),
            BoundExpr::AggFunc(agg_func) => write!(f, "{:?}", agg_func),
        }
    }
}

impl fmt::Debug for BoundColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.column_catalog)
    }
}

impl fmt::Debug for BoundInputRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InputRef#{}:{}", self.index, self.return_type)
    }
}

impl fmt::Debug for BoundTypeCast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cast({:?} as {})", self.expr, self.cast_type)
    }
}
