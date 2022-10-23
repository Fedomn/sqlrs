use std::sync::Arc;

use itertools::Itertools;

use super::RuleImpl;
use crate::binder::{BoundColumnRef, BoundExpr};
use crate::optimizer::core::{
    OptExpr, OptExprNode, Pattern, PatternChildrenPredicate, Rule, Substitute,
};
use crate::optimizer::rules::util::is_superset_cols;
use crate::optimizer::{Dummy, LogicalProject, LogicalTableScan, PlanNodeType};

lazy_static! {
    static ref PUSH_PROJECT_INTO_TABLE_SCAN_RULE: Pattern = {
        Pattern {
            predicate: |p| p.node_type() == PlanNodeType::LogicalProject,
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| p.node_type() == PlanNodeType::LogicalTableScan,
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
    static ref PUSH_PROJECT_THROUGH_CHILD_RULE: Pattern = {
        Pattern {
            predicate: |p| p.node_type() == PlanNodeType::LogicalProject,
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| p.node_type() != PlanNodeType::LogicalProject,
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
    static ref REMOVE_NOOP_OPERATORS_RULE: Pattern = {
        Pattern {
            predicate: |p| p.node_type() == PlanNodeType::LogicalProject,
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| {
                    p.node_type() == PlanNodeType::LogicalProject
                        || p.node_type() == PlanNodeType::LogicalAgg
                },
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

#[derive(Clone)]
pub struct PushProjectIntoTableScan;

impl PushProjectIntoTableScan {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for PushProjectIntoTableScan {
    fn pattern(&self) -> &Pattern {
        &PUSH_PROJECT_INTO_TABLE_SCAN_RULE
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        let project_opt_expr_root = opt_expr.root;
        let table_scan_opt_expr = opt_expr.children[0].clone();
        let project_node = project_opt_expr_root
            .get_plan_ref()
            .as_logical_project()
            .unwrap();

        // only push down the project when the exprs are all column refs
        for expr in project_node.exprs().iter() {
            match expr {
                BoundExpr::ColumnRef(_) => {}
                _ => return,
            }
        }

        let table_scan_node = table_scan_opt_expr
            .root
            .get_plan_ref()
            .as_logical_table_scan()
            .unwrap();

        let columns = project_node
            .exprs()
            .iter()
            .flat_map(|e| e.get_referenced_column_catalog())
            .collect::<Vec<_>>();
        let original_columns = table_scan_node.columns();
        let projections = columns
            .iter()
            .map(|c| original_columns.iter().position(|oc| oc == c).unwrap())
            .collect::<Vec<_>>();

        let new_table_scan_node = LogicalTableScan::new(
            table_scan_node.table_id(),
            table_scan_node.table_alias(),
            columns,
            table_scan_node.bounds(),
            Some(projections),
        );

        let new_table_scan_opt_expr = OptExpr::new(
            OptExprNode::PlanRef(Arc::new(new_table_scan_node)),
            table_scan_opt_expr.children,
        );

        result.opt_exprs.push(new_table_scan_opt_expr);
    }
}

/// Pushes a extra projection through a child node.
#[derive(Clone)]
pub struct PushProjectThroughChild;

impl PushProjectThroughChild {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for PushProjectThroughChild {
    fn pattern(&self) -> &Pattern {
        &PUSH_PROJECT_THROUGH_CHILD_RULE
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        let project_opt_expr_root = opt_expr.root;
        let child_opt_expr = opt_expr.children[0].clone();

        let project_plan_ref = &project_opt_expr_root.get_plan_ref();
        let project_cols = project_plan_ref.referenced_columns();
        let child_plan_ref = child_opt_expr.root.get_plan_ref();
        let child_cols = child_plan_ref.referenced_columns();
        let mut required_cols = [project_cols, child_cols].concat();
        let mut child_children_cols = child_plan_ref
            .children()
            .iter()
            .flat_map(|c| c.output_columns())
            .collect::<Vec<_>>();

        // distinct cols
        required_cols = required_cols.into_iter().unique().collect();
        child_children_cols = child_children_cols.into_iter().unique().collect();

        // println!("required_cols: {:?}", required_cols);
        // println!("child_children_cols: {:?}", child_children_cols);

        // if child_children_cols more than required_cols, pushdown extra projection.
        if is_superset_cols(&child_children_cols, &required_cols) {
            let new_child_opt_expr_children = child_plan_ref
                .children()
                .iter()
                .zip_eq(child_opt_expr.children.iter())
                .map(|(child_child_plan, child_child_opt_expr)| {
                    // Note: resolve base_table_id to calc real ColumnCatalog for subquery
                    // such as: select a, t2.v1 as max_b from t1 cross join (select max(b) as v1
                    // from t1) t2;
                    // `t2.v1` should be resolved in child_child_plan output_columns.
                    let mut child_child_output_cols = child_child_plan.output_columns();
                    // for child's child, filter corresponding required columns
                    let mut required_cols_in_child_child = child_child_output_cols
                        .clone()
                        .into_iter()
                        .filter(|c| required_cols.contains(c))
                        .collect::<Vec<_>>();

                    // distinct cols
                    child_child_output_cols =
                        child_child_output_cols.into_iter().unique().collect();
                    required_cols_in_child_child =
                        required_cols_in_child_child.into_iter().unique().collect();
                    // println!("child_child_output_cols: {:?}", child_child_output_cols);
                    // println!(
                    //     "required_cols_in_child_child: {:?}",
                    //     required_cols_in_child_child
                    // );

                    // if child's child cols more than required_cols, pushdown extra projection.
                    if is_superset_cols(&child_child_output_cols, &required_cols_in_child_child) {
                        let exprs = required_cols_in_child_child
                            .into_iter()
                            .map(|c| BoundExpr::ColumnRef(BoundColumnRef { column_catalog: c }))
                            .collect();
                        let new_project = LogicalProject::new(exprs, Dummy::new_ref());
                        OptExpr {
                            root: OptExprNode::PlanRef(Arc::new(new_project)),
                            children: vec![child_child_opt_expr.clone()],
                        }
                    } else {
                        child_child_opt_expr.clone()
                    }
                })
                .collect::<Vec<_>>();

            // TODO: use OptExpr directly, same refactoring for other rules
            let old_project_opt_expr_root = project_opt_expr_root.clone();
            let old_child_opt_expr_root = child_opt_expr.root.clone();

            let new_project_opt_expr = OptExpr::new(
                old_project_opt_expr_root,
                vec![OptExpr {
                    root: old_child_opt_expr_root,
                    children: new_child_opt_expr_children,
                }],
            );
            result.opt_exprs.push(new_project_opt_expr);
        }
    }
}

/// Remove no-op operators from the query plan that do not make any modifications.
#[derive(Clone)]
pub struct RemoveNoopOperators;

impl RemoveNoopOperators {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for RemoveNoopOperators {
    fn pattern(&self) -> &Pattern {
        &REMOVE_NOOP_OPERATORS_RULE
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        // eliminate no-op project for those children type: project{input: project/aggregate}
        let project_opt_expr_root = opt_expr.root;
        let project_plan_ref = project_opt_expr_root.get_plan_ref();
        let project_exprs = project_plan_ref.as_logical_project().unwrap().exprs();

        let child_opt_expr = opt_expr.children[0].clone().root;
        let child_opt_expr_children = opt_expr.children[0].clone().children;
        let child_plan_ref = &child_opt_expr.get_plan_ref();
        let child_exprs = match child_plan_ref.node_type() {
            PlanNodeType::LogicalProject => child_plan_ref.as_logical_project().unwrap().exprs(),
            PlanNodeType::LogicalAgg => {
                let plan = child_plan_ref.as_logical_agg().unwrap();
                [plan.group_by(), plan.agg_funcs()].concat()
            }
            _other => {
                unreachable!("RemoveNoopOperators not supprt type: {:?}", _other);
            }
        };

        if project_exprs == child_exprs {
            let new_opt_expr = OptExpr {
                root: child_opt_expr,
                children: child_opt_expr_children,
            };
            result.opt_exprs.push(new_opt_expr);
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::optimizer::rules::rule_test_util::{build_plan, RuleTest};
    use crate::optimizer::{
        HepBatch, HepBatchStrategy, HepOptimizer, PushProjectIntoTableScan,
        PushProjectThroughChild, RemoveNoopOperators,
    };
    use crate::util::pretty_plan_tree_string;

    #[test]
    fn test_push_project_into_table_scan_rule() {
        let tests = vec![
            RuleTest {
                name: "push_project_into_table_scan_rule",
                sql: "select a from t1",
                expect: "LogicalTableScan: table: #t1, columns: [a]",
            },
            RuleTest {
                name: "should not push when project has alias",
                sql: "select a as c1 from t1",
                expect: r"
LogicalProject: exprs [(t1.a:Nullable(Int32)) as t1.c1]
  LogicalTableScan: table: #t1, columns: [a, b, c]",
            },
            RuleTest {
                name: "should not push when project expr is not column",
                sql: "select a + 1 from t1",
                expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32) + 1]
  LogicalTableScan: table: #t1, columns: [a, b, c]",
            },
        ];

        for t in tests {
            let logical_plan = build_plan(t.sql);
            let batch = HepBatch::new(
                "Column Pruning".to_string(),
                HepBatchStrategy::fix_point_topdown(100),
                vec![PushProjectIntoTableScan::create()],
            );
            let mut optimizer = HepOptimizer::new(vec![batch], logical_plan);

            let optimized_plan = optimizer.find_best();

            let l = t.expect.trim_start();
            let r = pretty_plan_tree_string(&*optimized_plan);
            assert_eq!(l, r.trim_end(), "actual plan:\n{}", r);
        }
    }

    #[test]
    fn test_push_project_through_child_rule() {
        let tests = vec![
            RuleTest {
                name: "push_project_through_child_rule",
                sql: "select sum(b)+1 from t1 where a > 1",
                expect: r"
LogicalProject: exprs [Sum(t1.b:Nullable(Int32)):Int32 + 1]
  LogicalAgg: agg_funcs [Sum(t1.b:Nullable(Int32)):Int32] group_by []
    LogicalProject: exprs [t1.b:Nullable(Int32)]
      LogicalFilter: expr t1.a:Nullable(Int32) > 1
        LogicalTableScan: table: #t1, columns: [a, b]",
            },
            RuleTest {
                name: "push_project_through_child_rule to test RemoveNoopOperators",
                sql: "select sum(b) from t1 where a > 1",
                expect: r"
LogicalAgg: agg_funcs [Sum(t1.b:Nullable(Int32)):Int32] group_by []
  LogicalProject: exprs [t1.b:Nullable(Int32)]
    LogicalFilter: expr t1.a:Nullable(Int32) > 1
      LogicalTableScan: table: #t1, columns: [a, b]",
            },
            RuleTest {
                name: "push_project_through_child_rule for multiple join",
                sql: r"
select employee.id, employee.first_name, department.department_name, state.state_name, state.state_code from employee 
left join department on employee.department_id=department.id
right join state on state.state_code=employee.state;                
                ",
                expect: r"
LogicalProject: exprs [employee.id:Nullable(Int32), employee.first_name:Nullable(Int32), department.department_name:Nullable(Int32), state.state_name:Nullable(Int32), state.state_code:Nullable(Int32)]
  LogicalJoin: type Right, cond On { on: [(employee.state:Nullable(Int32), state.state_code:Nullable(Int32))], filter: None }
    LogicalProject: exprs [employee.id:Nullable(Int32), employee.first_name:Nullable(Int32), employee.state:Nullable(Int32), department.department_name:Nullable(Int32)]
      LogicalJoin: type Left, cond On { on: [(employee.department_id:Nullable(Int32), department.id:Nullable(Int32))], filter: None }
        LogicalTableScan: table: #employee, columns: [id, first_name, state, department_id]
        LogicalTableScan: table: #department, columns: [id, department_name]
    LogicalTableScan: table: #state, columns: [state_code, state_name]",
            },
        ];

        for t in tests {
            let logical_plan = build_plan(t.sql);
            let batch = HepBatch::new(
                "Column Pruning".to_string(),
                HepBatchStrategy::fix_point_topdown(100),
                vec![
                    PushProjectThroughChild::create(),
                    PushProjectIntoTableScan::create(),
                ],
            );
            let final_batch = HepBatch::new(
                "Remove noop operators".to_string(),
                HepBatchStrategy::fix_point_topdown(100),
                vec![RemoveNoopOperators::create()],
            );
            let mut optimizer = HepOptimizer::new(vec![batch, final_batch], logical_plan);

            let optimized_plan = optimizer.find_best();

            let l = t.expect.trim_start();
            let r = pretty_plan_tree_string(&*optimized_plan);
            assert_eq!(l, r.trim_end(), "actual plan:\n{}", r);
        }
    }
}
