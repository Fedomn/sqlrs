use sqlparser::ast::Query;

use crate::binder::{
    BindError, Binder, BoundAggFunc, BoundAlias, BoundBinaryOp, BoundColumnRef, BoundExpr,
    BoundSelect, BoundTableRef, BoundTypeCast, Join, JoinCondition, JoinType, TableSchema,
};
use crate::catalog::{ColumnCatalog, TableCatalog, TableId};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BoundSubqueryRef {
    pub query: Box<BoundSelect>,
    /// subquery always has a alias, if not, we will generate a alias number
    pub alias: TableId,
}

impl BoundSubqueryRef {
    pub fn new(query: Box<BoundSelect>, alias: TableId) -> Self {
        Self { query, alias }
    }

    fn get_output_columns(&self) -> Vec<ColumnCatalog> {
        self.query
            .select_list
            .iter()
            .map(|expr| {
                expr.output_column_catalog()
                    .clone_with_table_id(self.alias.clone())
            })
            .collect::<Vec<_>>()
    }

    pub fn gen_table_catalog_for_outside_reference(&self) -> TableCatalog {
        let subquery_output_columns = self.get_output_columns();
        TableCatalog::new_from_columns(self.alias.clone(), subquery_output_columns)
    }

    pub fn schema(&self) -> TableSchema {
        TableSchema::new_from_columns(self.get_output_columns())
    }

    pub fn bind_alias_to_all_columns(&mut self) {
        let table_catalog = self.gen_table_catalog_for_outside_reference();
        let column_catalog = table_catalog.get_all_columns();
        let new_subquery_select_list_with_alias = self
            .query
            .select_list
            .iter()
            .enumerate()
            .map(|(idx, expr)| {
                let column_catalog = column_catalog[idx].clone();
                BoundExpr::Alias(BoundAlias {
                    expr: Box::new(expr.clone()),
                    column_id: column_catalog.column_id,
                    table_id: column_catalog.table_id,
                })
            })
            .collect::<Vec<_>>();
        self.query.select_list = new_subquery_select_list_with_alias;
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BoundSubqueryExpr {
    pub query_ref: BoundSubqueryRef,
    pub kind: SubqueryKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SubqueryKind {
    /// Returns a scalar value
    Scalar,
}

impl BoundSubqueryExpr {
    pub fn new(query: Box<BoundSelect>, alias: TableId, kind: SubqueryKind) -> Self {
        Self {
            query_ref: BoundSubqueryRef::new(query, alias),
            kind,
        }
    }
}

impl Binder {
    pub fn bind_scalar_subquery(&mut self, subquery: &Query) -> Result<BoundExpr, BindError> {
        let bound_select = self.bind_select(subquery)?;
        Ok(BoundExpr::Subquery(BoundSubqueryExpr::new(
            Box::new(bound_select),
            self.gen_subquery_table_id(),
            SubqueryKind::Scalar,
        )))
    }

    fn gen_subquery_table_id(&mut self) -> String {
        let id = format!("subquery_{}", self.subquery_base_index);
        self.subquery_base_index += 1;
        id
    }

    /// Rewrite scalar subquery to join that includes:
    /// 1. replace scalar subquery with ColumnRef
    /// 2. add join to from_table
    pub fn rewrite_scalar_subquery(
        &mut self,
        expr: &mut BoundExpr,
        from_table: &mut Option<BoundTableRef>,
    ) {
        if expr.contains_subquery() {
            let bound_table_ref = from_table
                .clone()
                .unwrap_or_else(|| todo!("need logical values"));
            if let Some((new_expr, new_table_ref)) =
                self.rewrite_scalar_subquery_to_join(&*expr, &bound_table_ref)
            {
                *expr = new_expr;
                *from_table = Some(new_table_ref)
            }
        }
    }

    fn rewrite_scalar_subquery_to_join(
        &mut self,
        scalar_subquery_expr: &BoundExpr,
        base_table_ref: &BoundTableRef,
    ) -> Option<(BoundExpr, BoundTableRef)> {
        // rewrite uncorrelated scalar subquery to cross join
        let subqueries = scalar_subquery_expr.get_scalar_subquery();
        if subqueries.is_empty() {
            return None;
        }

        let mut new_scalar_subquery_expr = scalar_subquery_expr.clone();
        let mut new_base_table_ref = base_table_ref.clone();
        for mut subquery in subqueries {
            let subquery_table_id = subquery.query_ref.alias.clone();
            let column_id = format!("{}_{}", subquery_table_id, "scalar_v0");
            let replaced_col_ref = BoundExpr::ColumnRef(BoundColumnRef {
                column_catalog: ColumnCatalog::new(
                    subquery_table_id.clone(),
                    column_id.clone(),
                    true,
                    subquery.query_ref.query.select_list[0]
                        .return_type()
                        .unwrap(),
                ),
            });

            new_scalar_subquery_expr = new_scalar_subquery_expr
                .replace_subquery_with_new_expr(&subquery, &replaced_col_ref);
            let new_subquery_select_expr = BoundExpr::Alias(BoundAlias {
                expr: Box::new(subquery.query_ref.query.select_list[0].clone()),
                column_id,
                table_id: subquery_table_id.clone(),
            });
            subquery.query_ref.query.select_list = vec![new_subquery_select_expr];
            new_base_table_ref = BoundTableRef::Join(Join {
                left: Box::new(new_base_table_ref),
                right: Box::new(BoundTableRef::Subquery(BoundSubqueryRef::new(
                    subquery.query_ref.query,
                    subquery_table_id,
                ))),
                join_type: JoinType::Cross,
                join_condition: JoinCondition::None,
            });
        }

        Some((new_scalar_subquery_expr, new_base_table_ref))
    }
}

impl BoundExpr {
    // pub fn contains_scalar_subquery(&self) -> bool {
    //     match self {
    //         BoundExpr::Constant(_) | BoundExpr::ColumnRef(_) | BoundExpr::InputRef(_) => false,
    //         BoundExpr::BinaryOp(binary_op) => {
    //             binary_op.left.contains_scalar_subquery()
    //                 || binary_op.right.contains_scalar_subquery()
    //         }
    //         BoundExpr::TypeCast(tc) => tc.expr.contains_scalar_subquery(),
    //         BoundExpr::AggFunc(agg) => agg.exprs.iter().any(|arg|
    // arg.contains_scalar_subquery()),         BoundExpr::Alias(alias) =>
    // alias.expr.contains_scalar_subquery(),         BoundExpr::Subquery(_) => true,
    //     }
    // }

    pub fn get_scalar_subquery(&self) -> Vec<BoundSubqueryExpr> {
        match self {
            BoundExpr::Constant(_) | BoundExpr::InputRef(_) | BoundExpr::ColumnRef(_) => vec![],
            BoundExpr::BinaryOp(binary_op) => binary_op
                .left
                .get_scalar_subquery()
                .into_iter()
                .chain(binary_op.right.get_scalar_subquery().into_iter())
                .collect(),
            BoundExpr::TypeCast(tc) => tc.expr.get_scalar_subquery(),
            BoundExpr::AggFunc(agg) => agg
                .exprs
                .iter()
                .flat_map(|arg| arg.get_scalar_subquery())
                .collect(),
            BoundExpr::Alias(alias) => alias.expr.get_scalar_subquery(),
            BoundExpr::Subquery(query) => vec![query.clone()],
        }
    }

    pub fn replace_subquery_with_new_expr(
        &self,
        replaced_subquery: &BoundSubqueryExpr,
        new_expr: &BoundExpr,
    ) -> BoundExpr {
        match self {
            BoundExpr::Constant(_) | BoundExpr::InputRef(_) | BoundExpr::ColumnRef(_) => {
                self.clone()
            }
            BoundExpr::BinaryOp(binary_op) => BoundExpr::BinaryOp(BoundBinaryOp {
                left: Box::new(
                    binary_op
                        .left
                        .replace_subquery_with_new_expr(replaced_subquery, new_expr),
                ),
                op: binary_op.op.clone(),
                right: Box::new(
                    binary_op
                        .right
                        .replace_subquery_with_new_expr(replaced_subquery, new_expr),
                ),
                return_type: binary_op.return_type.clone(),
            }),
            BoundExpr::TypeCast(tc) => BoundExpr::TypeCast(BoundTypeCast {
                expr: Box::new(
                    tc.expr
                        .replace_subquery_with_new_expr(replaced_subquery, new_expr),
                ),
                cast_type: tc.cast_type.clone(),
            }),
            BoundExpr::AggFunc(agg) => BoundExpr::AggFunc(BoundAggFunc {
                distinct: agg.distinct,
                exprs: agg
                    .exprs
                    .iter()
                    .map(|arg| arg.replace_subquery_with_new_expr(replaced_subquery, new_expr))
                    .collect(),
                func: agg.func.clone(),
                return_type: agg.return_type.clone(),
            }),
            BoundExpr::Alias(alias) => BoundExpr::Alias(BoundAlias {
                expr: Box::new(
                    alias
                        .expr
                        .replace_subquery_with_new_expr(replaced_subquery, new_expr),
                ),
                column_id: alias.column_id.clone(),
                table_id: alias.table_id.clone(),
            }),
            BoundExpr::Subquery(query) => {
                if query == replaced_subquery {
                    new_expr.clone()
                } else {
                    self.clone()
                }
            }
        }
    }
}
