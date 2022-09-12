use std::sync::Arc;
use std::vec;

use arrow::datatypes::DataType;
use sqlparser::ast::BinaryOperator;

use super::util::is_subset_cols;
use super::RuleImpl;
use crate::binder::{BoundBinaryOp, BoundExpr, JoinType};
use crate::optimizer::core::*;
use crate::optimizer::{Dummy, LogicalFilter, LogicalJoin, PlanNodeType};

lazy_static! {
    static ref PUSH_PREDICATE_THROUGH_JOIN: Pattern = {
        Pattern {
            predicate: |p| p.node_type() == PlanNodeType::LogicalFilter,
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| p.node_type() == PlanNodeType::LogicalJoin,
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

/// Comments copied from Spark Catalyst PushPredicateThroughJoin
///
/// Pushes down `Filter` operators where the `condition` can be
/// evaluated using only the attributes of the left or right side of a join.  Other
/// `Filter` conditions are moved into the `condition` of the `Join`.
///
/// And also pushes down the join filter, where the `condition` can be evaluated using only the
/// attributes of the left or right side of sub query when applicable.
#[derive(Clone)]
pub struct PushPredicateThroughJoin;

impl PushPredicateThroughJoin {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }

    fn can_push_through(&self, join_type: JoinType) -> bool {
        matches!(
            join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right
        )
    }

    fn split_conjunctive_predicates(&self, expr: &BoundExpr) -> Vec<BoundExpr> {
        match expr {
            BoundExpr::BinaryOp(BoundBinaryOp {
                op: BinaryOperator::And,
                left,
                right,
                ..
            }) => [
                self.split_conjunctive_predicates(left),
                self.split_conjunctive_predicates(right),
            ]
            .concat(),
            _ => vec![expr.clone()],
        }
    }

    /// reduce filters into a filter, and then build a new LogicalFilter node with input child.
    /// if filters is empty, return the input child.
    fn reduce_filters_with_child_into_opt_expr(
        &self,
        filters: Vec<BoundExpr>,
        child_opt_expr: OptExpr,
    ) -> OptExpr {
        filters
            .into_iter()
            .reduce(|a, b| {
                BoundExpr::BinaryOp(BoundBinaryOp {
                    op: BinaryOperator::And,
                    left: Box::new(a),
                    right: Box::new(b),
                    return_type: Some(DataType::Boolean),
                })
            })
            .map(|f| {
                OptExpr::new(
                    OptExprNode::PlanRef(Arc::new(LogicalFilter::new(f, Dummy::new_ref()))),
                    vec![child_opt_expr.clone()],
                )
            })
            .unwrap_or(child_opt_expr)
    }
}

impl Rule for PushPredicateThroughJoin {
    fn pattern(&self) -> &Pattern {
        &PUSH_PREDICATE_THROUGH_JOIN
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        let join_opt_expr = opt_expr.children[0].clone();
        let join_node = join_opt_expr.root.get_plan_ref().as_logical_join().unwrap();
        if !self.can_push_through(join_node.join_type()) {
            return;
        }

        let left_output_cols = join_node.left().output_columns();
        let right_output_cols = join_node.right().output_columns();

        let filter_opt_expr = opt_expr;
        let join_left_opt_expr = join_opt_expr.children[0].clone();
        let join_right_opt_expr = join_opt_expr.children[1].clone();

        let filter_expr = filter_opt_expr
            .root
            .get_plan_ref()
            .as_logical_filter()
            .unwrap()
            .expr();

        let filter_exprs = self.split_conjunctive_predicates(&filter_expr);
        let (left_filters, rest): (Vec<_>, Vec<_>) = filter_exprs
            .into_iter()
            .partition(|f| is_subset_cols(&f.get_column_catalog(), &left_output_cols));
        let (right_filters, common_filters): (Vec<_>, Vec<_>) = rest
            .into_iter()
            .partition(|f| is_subset_cols(&f.get_column_catalog(), &right_output_cols));

        match join_node.join_type() {
            JoinType::Inner => {
                // push down the single side `where` condition into respective sides
                let new_left =
                    self.reduce_filters_with_child_into_opt_expr(left_filters, join_left_opt_expr);
                let new_right = self
                    .reduce_filters_with_child_into_opt_expr(right_filters, join_right_opt_expr);

                // merge common_filters into join_condition
                let new_join_condition = join_node.join_condition().add_new_filters(common_filters);
                let new_join_root = OptExprNode::PlanRef(Arc::new(LogicalJoin::new(
                    Dummy::new_ref(),
                    Dummy::new_ref(),
                    join_node.join_type(),
                    new_join_condition,
                )));

                let res = OptExpr::new(new_join_root, vec![new_left, new_right]);
                result.opt_exprs.push(res);
            }
            JoinType::Left => {
                // push down the left side only `where` condition
                let new_left_opt_expr =
                    self.reduce_filters_with_child_into_opt_expr(left_filters, join_left_opt_expr);

                let new_join_opt_expr = OptExpr::new(
                    join_opt_expr.root,
                    vec![new_left_opt_expr, join_right_opt_expr],
                );
                let res = self.reduce_filters_with_child_into_opt_expr(
                    [right_filters, common_filters].concat(),
                    new_join_opt_expr,
                );
                result.opt_exprs.push(res);
            }
            JoinType::Right => {
                // push down the right side only `where` condition
                let new_right_opt_expr = self
                    .reduce_filters_with_child_into_opt_expr(right_filters, join_right_opt_expr);

                let new_join_opt_expr = OptExpr::new(
                    join_opt_expr.root,
                    vec![join_left_opt_expr, new_right_opt_expr],
                );
                let res = self.reduce_filters_with_child_into_opt_expr(
                    [left_filters, common_filters].concat(),
                    new_join_opt_expr,
                );
                result.opt_exprs.push(res);
            }
            _ => unreachable!("should not reach here"),
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::PushPredicateThroughJoin;
    use crate::optimizer::rules::rule_test_util::{build_plan, RuleTest};
    use crate::optimizer::{HepBatch, HepBatchStrategy, HepOptimizer};
    use crate::util::pretty_plan_tree_string;

    #[test]
    fn test_push_predicate_through_join_rule() {
        let tests = vec![
            RuleTest {
                name: "joins: push to either side",
                sql: "select t1.* from t1 inner join t2 on t1.a=t2.b where t2.a>2 and t1.a>1",
                expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32), t1.b:Nullable(Int32), t1.c:Nullable(Int32)]
  LogicalJoin: type Inner, cond On { on: [(t1.a:Nullable(Int32), t2.b:Nullable(Int32))], filter: None }
    LogicalFilter: expr t1.a:Nullable(Int32) > 1
      LogicalTableScan: table: #t1, columns: [a, b, c]
    LogicalFilter: expr t2.a:Nullable(Int32) > 2
      LogicalTableScan: table: #t2, columns: [a, b, c]",
            },
            RuleTest {
                name: "joins: push down left outer join",
                sql: "select t1.* from t1 left join t2 on t1.a=t2.b where t2.a>2 and t1.a>1",
                expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32), t1.b:Nullable(Int32), t1.c:Nullable(Int32)]
  LogicalFilter: expr t2.a:Nullable(Int32) > 2
    LogicalJoin: type Left, cond On { on: [(t1.a:Nullable(Int32), t2.b:Nullable(Int32))], filter: None }
      LogicalFilter: expr t1.a:Nullable(Int32) > 1
        LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]",
            },
            RuleTest {
                name: "joins: push down right outer join",
                sql: "select t1.* from t1 right join t2 on t1.a=t2.b where t2.a>2 and t1.a>1",
                expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32), t1.b:Nullable(Int32), t1.c:Nullable(Int32)]
  LogicalFilter: expr t1.a:Nullable(Int32) > 1
    LogicalJoin: type Right, cond On { on: [(t1.a:Nullable(Int32), t2.b:Nullable(Int32))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalFilter: expr t2.a:Nullable(Int32) > 2
        LogicalTableScan: table: #t2, columns: [a, b, c]",
            },
            RuleTest {
                name: "joins: push down common filters into join condition",
                sql: "select t1.* from t1 inner join t2 on t1.a=t2.b where t2.a>2 and t1.a>t2.a",
                expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32), t1.b:Nullable(Int32), t1.c:Nullable(Int32)]
  LogicalJoin: type Inner, cond On { on: [(t1.a:Nullable(Int32), t2.b:Nullable(Int32))], filter: Some(t1.a:Nullable(Int32) > t2.a:Nullable(Int32)) }
    LogicalTableScan: table: #t1, columns: [a, b, c]
    LogicalFilter: expr t2.a:Nullable(Int32) > 2
      LogicalTableScan: table: #t2, columns: [a, b, c]",
            },
            RuleTest {
                name: "joins: don't push down filters for left outer join",
                sql: "select t1.* from t1 left join t2 on t1.a=t2.b where t2.a>2 and t1.a>t2.a",
                expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32), t1.b:Nullable(Int32), t1.c:Nullable(Int32)]
  LogicalFilter: expr t2.a:Nullable(Int32) > 2 AND t1.a:Nullable(Int32) > t2.a:Nullable(Int32)
    LogicalJoin: type Left, cond On { on: [(t1.a:Nullable(Int32), t2.b:Nullable(Int32))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]",
            },
            RuleTest {
                name: "joins: don't push down filters for right outer join",
                sql: "select t1.* from t1 right join t2 on t1.a=t2.b where t1.a>2 and t1.a>t2.a",
                expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32), t1.b:Nullable(Int32), t1.c:Nullable(Int32)]
  LogicalFilter: expr t1.a:Nullable(Int32) > 2 AND t1.a:Nullable(Int32) > t2.a:Nullable(Int32)
    LogicalJoin: type Right, cond On { on: [(t1.a:Nullable(Int32), t2.b:Nullable(Int32))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]",
            },
        ];

        for t in tests {
            let logical_plan = build_plan(t.sql);
            let batch = HepBatch::new(
                "Operator push down".to_string(),
                HepBatchStrategy::fix_point_topdown(100),
                vec![PushPredicateThroughJoin::create()],
            );
            let mut optimizer = HepOptimizer::new(vec![batch], logical_plan);

            let optimized_plan = optimizer.find_best();

            let l = t.expect.trim_start();
            let r = pretty_plan_tree_string(&*optimized_plan);
            assert_eq!(l, r.trim_end(), "actual plan:\n{}", r);
        }
    }
}
