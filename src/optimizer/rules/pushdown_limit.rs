use std::sync::Arc;

use super::RuleImpl;
use crate::binder::{BoundExpr, JoinCondition, JoinType};
use crate::optimizer::core::{
    OptExpr, OptExprNode, Pattern, PatternChildrenPredicate, Rule, Substitute,
};
use crate::optimizer::{Dummy, LogicalLimit, LogicalTableScan, PlanNodeType};

lazy_static! {
    static ref LIMIT_PROJECT_TRANSPOSE_RULE: Pattern = {
        Pattern {
            predicate: |p| p.node_type() == PlanNodeType::LogicalLimit,
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| p.node_type() == PlanNodeType::LogicalProject,
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
    static ref ELIMINATE_LIMITS_RULE: Pattern = {
        Pattern {
            predicate: |p| p.node_type() == PlanNodeType::LogicalLimit,
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| p.node_type() == PlanNodeType::LogicalLimit,
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
    static ref PUSH_LIMIT_THROUGH_JOIN_RULE: Pattern = {
        Pattern {
            predicate: |p| p.node_type() == PlanNodeType::LogicalLimit,
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| p.node_type() == PlanNodeType::LogicalJoin,
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
    static ref PUSH_LIMIT_INTO_TABLE_SCAN_RULE: Pattern = {
        Pattern {
            predicate: |p| p.node_type() == PlanNodeType::LogicalLimit,
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| p.node_type() == PlanNodeType::LogicalTableScan,
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

/// Push down `Limit` past a `Project`.
#[derive(Clone)]
pub struct LimitProjectTranspose;

impl LimitProjectTranspose {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for LimitProjectTranspose {
    fn pattern(&self) -> &Pattern {
        &LIMIT_PROJECT_TRANSPOSE_RULE
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        let limit_opt_expr_root = opt_expr.root;
        let project_opt_expr = opt_expr.children[0].clone();

        let new_project_opt_expr = OptExpr::new(
            project_opt_expr.root,
            vec![OptExpr::new(limit_opt_expr_root, project_opt_expr.children)],
        );

        result.opt_exprs.push(new_project_opt_expr);
    }
}

/// Combines two adjacent Limit operators into one, merging the expressions into one single
/// expression.
#[derive(Clone)]
pub struct EliminateLimits;

impl EliminateLimits {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for EliminateLimits {
    fn pattern(&self) -> &Pattern {
        &ELIMINATE_LIMITS_RULE
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        let limit_opt_expr_root = opt_expr.root;
        let next_limit_opt_expr = opt_expr.children[0].clone();
        let next_limit_opt_expr_root = next_limit_opt_expr.root;

        let limit_node = limit_opt_expr_root
            .get_plan_ref()
            .as_logical_limit()
            .unwrap();
        let next_limit_node = next_limit_opt_expr_root
            .get_plan_ref()
            .as_logical_limit()
            .unwrap();
        let new_limit = match (limit_node.limit(), next_limit_node.limit()) {
            (Some(BoundExpr::Constant(a)), Some(BoundExpr::Constant(b))) => {
                Some(BoundExpr::Constant(
                    (a.as_usize().unwrap().min(b.as_usize().unwrap()) as i32).into(),
                ))
            }
            (Some(_), Some(_)) => unreachable!("not support limit expr"),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let new_offset = match (limit_node.offset(), next_limit_node.offset()) {
            (Some(BoundExpr::Constant(a)), Some(BoundExpr::Constant(b))) => {
                Some(BoundExpr::Constant(
                    (a.as_usize().unwrap() as i32 + b.as_usize().unwrap() as i32).into(),
                ))
            }
            (Some(_), Some(_)) => unreachable!("not support limit expr"),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };

        let new_limit_opt_expr = OptExprNode::PlanRef(Arc::new(LogicalLimit::new(
            new_limit,
            new_offset,
            Dummy::new_ref(),
        )));

        let new_opt_expr = OptExpr::new(new_limit_opt_expr, next_limit_opt_expr.children);
        result.opt_exprs.push(new_opt_expr);
    }
}

/// Add extra limits below JOIN:
/// 1. For LEFT OUTER and RIGHT OUTER JOIN, we push limits to the left and right sides,
/// respectively.
/// 2. For INNER and CROSS JOIN, we push limits to both the left and right sides if join condition
/// is empty.
#[derive(Clone)]
pub struct PushLimitThroughJoin;

impl PushLimitThroughJoin {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for PushLimitThroughJoin {
    fn pattern(&self) -> &Pattern {
        &PUSH_LIMIT_THROUGH_JOIN_RULE
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        let limit_opt_expr_root = opt_expr.root;
        let limit_node = limit_opt_expr_root
            .get_plan_ref()
            .as_logical_limit()
            .unwrap();

        if limit_node.limit().is_none() {
            return;
        }

        let pushdown_limit_row_count = match (limit_node.limit(), limit_node.offset()) {
            (Some(BoundExpr::Constant(limit)), Some(BoundExpr::Constant(offset))) => {
                BoundExpr::Constant(
                    (limit.as_usize().unwrap() as i32 + offset.as_usize().unwrap() as i32).into(),
                )
            }
            (Some(_), Some(_)) => unreachable!("not support limit expr"),
            (Some(limit), None) => limit,
            (None, _) => unreachable!("pushdown must have limit"),
        };
        let pushdown_limit_node =
            LogicalLimit::new(Some(pushdown_limit_row_count), None, Dummy::new_ref());
        let pushdown_limit_opt_expr = OptExprNode::PlanRef(Arc::new(pushdown_limit_node));

        let join_opt_expr = opt_expr.children[0].clone();
        let join_node = join_opt_expr.root.get_plan_ref().as_logical_join().unwrap();
        match join_node.join_type() {
            JoinType::Left => {
                let new_join_opt_expr = OptExpr::new(
                    join_opt_expr.root,
                    vec![
                        OptExpr::new(
                            pushdown_limit_opt_expr,
                            vec![join_opt_expr.children[0].clone()],
                        ),
                        join_opt_expr.children[1].clone(),
                    ],
                );
                let new_limit_opt_expr = OptExpr::new(limit_opt_expr_root, vec![new_join_opt_expr]);
                result.opt_exprs.push(new_limit_opt_expr);
            }
            JoinType::Right => {
                let new_join_opt_expr = OptExpr::new(
                    join_opt_expr.root,
                    vec![
                        join_opt_expr.children[0].clone(),
                        OptExpr::new(
                            pushdown_limit_opt_expr,
                            vec![join_opt_expr.children[1].clone()],
                        ),
                    ],
                );
                let new_limit_opt_expr = OptExpr::new(limit_opt_expr_root, vec![new_join_opt_expr]);
                result.opt_exprs.push(new_limit_opt_expr);
            }
            JoinType::Inner | JoinType::Cross => {
                if join_node.join_condition() == JoinCondition::None {
                    todo!("currently, not support none condition join");
                }
            }
            _ => (),
        }
    }
}

/// Push down `Limit` past a `Project`.
#[derive(Clone)]
pub struct PushLimitIntoTableScan;

impl PushLimitIntoTableScan {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for PushLimitIntoTableScan {
    fn pattern(&self) -> &Pattern {
        &PUSH_LIMIT_INTO_TABLE_SCAN_RULE
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        let limit_opt_expr_root = opt_expr.root;
        let limit_node = limit_opt_expr_root
            .get_plan_ref()
            .as_logical_limit()
            .unwrap();

        let table_scan_opt_expr = opt_expr.children[0].clone();
        let table_scan_node = table_scan_opt_expr
            .root
            .get_plan_ref()
            .as_logical_table_scan()
            .unwrap();

        let bounds = match (limit_node.offset(), limit_node.limit()) {
            (Some(BoundExpr::Constant(offset)), Some(BoundExpr::Constant(limit))) => {
                (offset.as_usize().unwrap(), limit.as_usize().unwrap())
            }
            (Some(BoundExpr::Constant(offset)), None) => (offset.as_usize().unwrap(), usize::MAX),
            (None, Some(BoundExpr::Constant(limit))) => (0, limit.as_usize().unwrap()),
            _ => unreachable!("not support limit expr"),
        };

        let new_table_scan_node = LogicalTableScan::new(
            table_scan_node.table_id(),
            table_scan_node.table_alias(),
            table_scan_node.columns(),
            Some(bounds),
            table_scan_node.projections(),
        );

        let new_table_scan_opt_expr = OptExpr::new(
            OptExprNode::PlanRef(Arc::new(new_table_scan_node)),
            table_scan_opt_expr.children,
        );

        result.opt_exprs.push(new_table_scan_opt_expr);
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::optimizer::rules::rule_test_util::{build_plan, RuleTest};
    use crate::optimizer::{
        EliminateLimits, HepBatch, HepBatchStrategy, HepOptimizer, LimitProjectTranspose,
        PushLimitIntoTableScan, PushLimitThroughJoin,
    };
    use crate::util::pretty_plan_tree_string;

    #[test]
    fn test_limit_project_transpose_rule() {
        let tests = vec![RuleTest {
            name: "limit_project_transpose_rule",
            sql: "select a from t1 offset 2 limit 1",
            expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32)]
  LogicalLimit: limit Some(1), offset Some(2)
    LogicalTableScan: table: #t1, columns: [a, b, c]",
        }];

        for t in tests {
            let logical_plan = build_plan(t.sql);
            let batch = HepBatch::new(
                "Operator push down".to_string(),
                HepBatchStrategy::fix_point_topdown(100),
                vec![LimitProjectTranspose::create()],
            );
            let mut optimizer = HepOptimizer::new(vec![batch], logical_plan);

            let optimized_plan = optimizer.find_best();

            let l = t.expect.trim_start();
            let r = pretty_plan_tree_string(&*optimized_plan);
            assert_eq!(l, r.trim_end(), "actual plan:\n{}", r);
        }
    }

    #[test]
    fn test_push_limit_through_join_rule() {
        let tests = vec![
            RuleTest {
                name: "push_limit_through_join for left outer join",
                sql: "select t1.a from t1 left join t2 on t1.a=t2.b offset 1 limit 1",
                expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32)]
  LogicalLimit: limit Some(1), offset Some(1)
    LogicalJoin: type Left, cond On { on: [(t1.a:Nullable(Int32), t2.b:Nullable(Int32))], filter: None }
      LogicalLimit: limit Some(2), offset None
        LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]",
            },
            RuleTest {
                name: "push_limit_through_join for right outer join",
                sql: "select t1.a from t1 right join t2 on t1.a=t2.b limit 1",
                expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32)]
  LogicalLimit: limit Some(1), offset None
    LogicalJoin: type Right, cond On { on: [(t1.a:Nullable(Int32), t2.b:Nullable(Int32))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalLimit: limit Some(1), offset None
        LogicalTableScan: table: #t2, columns: [a, b, c]",
            },
            RuleTest {
                name: "don't push_limit_through_join when not contains limit",
                sql: "select t1.a from t1 right join t2 on t1.a=t2.b offset 10",
                expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32)]
  LogicalLimit: limit None, offset Some(10)
    LogicalJoin: type Right, cond On { on: [(t1.a:Nullable(Int32), t2.b:Nullable(Int32))], filter: None }
      LogicalTableScan: table: #t1, columns: [a, b, c]
      LogicalTableScan: table: #t2, columns: [a, b, c]",
            },
        ];

        for t in tests {
            let logical_plan = build_plan(t.sql);
            let batch = HepBatch::new(
                "Operator push down".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    LimitProjectTranspose::create(),
                    PushLimitThroughJoin::create(),
                    EliminateLimits::create(),
                ],
            );
            let mut optimizer = HepOptimizer::new(vec![batch], logical_plan);

            let optimized_plan = optimizer.find_best();

            let l = t.expect.trim_start();
            let r = pretty_plan_tree_string(&*optimized_plan);
            assert_eq!(l, r.trim_end(), "actual plan:\n{}", r);
        }
    }

    #[test]
    fn test_push_limit_into_table_scan() {
        let tests = vec![RuleTest {
            name: "limit_project_transpose_rule",
            sql: "select a from t1 offset 2 limit 1",
            expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32)]
  LogicalTableScan: table: #t1, columns: [a, b, c], bounds: (offset:2,limit:1)",
        }];

        for t in tests {
            let logical_plan = build_plan(t.sql);
            let batch = HepBatch::new(
                "Operator push down".to_string(),
                HepBatchStrategy::fix_point_topdown(100),
                vec![
                    LimitProjectTranspose::create(),
                    PushLimitIntoTableScan::create(),
                ],
            );
            let mut optimizer = HepOptimizer::new(vec![batch], logical_plan);

            let optimized_plan = optimizer.find_best();

            let l = t.expect.trim_start();
            let r = pretty_plan_tree_string(&*optimized_plan);
            assert_eq!(l, r.trim_end(), "actual plan:\n{}", r);
        }
    }
}
