mod binary_op;

use std::slice;

use arrow::datatypes::DataType;
use itertools::Itertools;
use sqlparser::ast::{Expr, Ident};

use crate::{catalog::ColumnCatalog, types::ScalarValue};

use self::binary_op::BoundBinaryOp;

use super::{BindError, Binder};

#[derive(Debug)]
pub enum BoundExpr {
    Constant(ScalarValue),
    ColumnRef(BoundColumnRef),
    InputRef(BoundInputRef),
    BinaryOp(BoundBinaryOp),
}

impl BoundExpr {
    pub fn return_type(&self) -> Option<DataType> {
        match self {
            BoundExpr::Constant(value) => Some(value.data_type()),
            BoundExpr::ColumnRef(column_ref) => {
                Some(column_ref.column_catalog.desc.data_type.clone())
            }
            BoundExpr::InputRef(input_ref) => Some(input_ref.return_type.clone()),
            BoundExpr::BinaryOp(binary_op) => binary_op.return_type.clone(),
        }
    }
}

#[derive(Debug)]
pub struct BoundColumnRef {
    pub column_catalog: ColumnCatalog,
}

#[derive(Debug)]
pub struct BoundInputRef {
    /// column index in data chunk
    pub index: usize,
    pub return_type: DataType,
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
            _ => todo!("unsupported expr {:?}", expr),
        }
    }

    /// bind sqlparser Identifier into BoundExpr
    ///
    /// Identifier types:
    ///  * Identifier(Ident): Identifier e.g. table name or column name
    ///  * CompoundIdentifier(Vec<Ident>): Multi-part identifier, e.g. `table_alias.column` or `schema.table.col`
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
            let table_catalog = self.context.tables.get(table).unwrap();
            let column_catalog = table_catalog.get_column_by_name(column_name).unwrap();
            Ok(BoundExpr::ColumnRef(BoundColumnRef { column_catalog }))
        } else {
            let mut got_column = None;
            for (_table_name, table_catalog) in &self.context.tables {
                // TODO: add ambiguous column check
                got_column = Some(table_catalog.get_column_by_name(column_name).unwrap());
            }
            let column_catalog =
                got_column.ok_or_else(|| BindError::InvalidColumn(column_name.clone()))?;
            Ok(BoundExpr::ColumnRef(BoundColumnRef { column_catalog }))
        }
    }
}
