use std::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};
use crate::binder::{JoinCondition, JoinType};
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct LogicalJoin {
    left: PlanRef,
    right: PlanRef,
    join_type: JoinType,
    join_condition: JoinCondition,
    /// The join output columns is special to record the output nullable columns and prepare for
    /// the join executor.
    join_output_columns: Vec<ColumnCatalog>,
}

impl LogicalJoin {
    pub fn new(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        join_condition: JoinCondition,
    ) -> Self {
        let mut join = Self {
            left,
            right,
            join_type,
            join_condition,
            join_output_columns: vec![],
        };
        join.join_output_columns = join.join_output_columns_internal();
        join
    }

    /// Used in InputRefRewriter to record the join output columns.
    pub fn new_with_output_columns(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        join_condition: JoinCondition,
        join_output_columns: Vec<ColumnCatalog>,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            join_condition,
            join_output_columns,
        }
    }

    pub fn left(&self) -> PlanRef {
        self.left.clone()
    }

    pub fn right(&self) -> PlanRef {
        self.right.clone()
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type.clone()
    }

    pub fn join_condition(&self) -> JoinCondition {
        self.join_condition.clone()
    }

    pub fn join_output_columns(&self) -> Vec<ColumnCatalog> {
        self.join_output_columns.clone()
    }

    /// To handle multiple join conditions, such as:
    /// `select * from a left join b on a.id = b.id inner join c on b.id = c.id`
    ///
    /// The left child is: `a left join b`
    /// The right child is: `c`
    ///
    /// So in the left child schema, b's fields is nullable, therefore we should use left join
    /// schema directly, rather than set b's fields as non-nullable.
    fn join_output_columns_internal(&self) -> Vec<ColumnCatalog> {
        let (left_join_keys_force_nullable, right_join_keys_force_nullable) = match self.join_type {
            JoinType::Inner => (false, false),
            JoinType::Left => (false, true),
            JoinType::Right => (true, false),
            JoinType::Full => (true, true),
            JoinType::Cross => (true, true),
        };
        let left_fields = self
            .left
            .output_columns()
            .iter()
            .map(|c| {
                c.clone_with_nullable(
                    // if force nullable is false, use the original value
                    // to handle some original fields that are nullable
                    left_join_keys_force_nullable || c.nullable,
                )
            })
            .collect::<Vec<_>>();
        let right_fields = self
            .right
            .output_columns()
            .iter()
            .map(|c| {
                c.clone_with_nullable(
                    // if force nullable is false, use the original value
                    // to handle some original fields that are nullable
                    right_join_keys_force_nullable || c.nullable,
                )
            })
            .collect::<Vec<_>>();

        vec![left_fields, right_fields].concat()
    }
}

impl PlanNode for LogicalJoin {
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        match self.join_condition() {
            JoinCondition::On { on, filter } => {
                let on_cols = on
                    .iter()
                    .flat_map(|e| {
                        [
                            e.0.get_referenced_column_catalog(),
                            e.1.get_referenced_column_catalog(),
                        ]
                        .concat()
                    })
                    .collect::<Vec<_>>();
                let filter_cols = filter
                    .map(|f| f.get_referenced_column_catalog())
                    .unwrap_or_default();
                [on_cols, filter_cols].concat()
            }
            JoinCondition::None => vec![],
        }
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        self.join_output_columns_internal()
    }
}

impl PlanTreeNode for LogicalJoin {
    fn children(&self) -> Vec<PlanRef> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        assert_eq!(children.len(), 2);
        Arc::new(Self::new(
            children[0].clone(),
            children[1].clone(),
            self.join_type.clone(),
            self.join_condition.clone(),
        ))
    }
}

impl fmt::Display for LogicalJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalJoin: type {:?}, cond {:?}",
            self.join_type, self.join_condition
        )
    }
}

impl PartialEq for LogicalJoin {
    fn eq(&self, other: &Self) -> bool {
        self.join_type == other.join_type
            && self.join_condition == other.join_condition
            && self.left == other.left()
            && self.right == other.right()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::test_util::*;
    use crate::optimizer::LogicalTableScan;

    #[test]
    fn test_join_output_schema_when_two_tables() {
        let t1 = Arc::new(LogicalTableScan::new(
            "t1".to_string(),
            None,
            build_columns_catalog("t1", vec!["a1", "b1", "c1"], false),
            None,
            None,
        ));
        let t2 = Arc::new(LogicalTableScan::new(
            "t2".to_string(),
            None,
            build_columns_catalog("t2", vec!["a2", "b1", "c2"], false),
            None,
            None,
        ));
        let cond = build_join_condition_eq("t1", "b1", "t2", "b1");

        let plan = LogicalJoin::new(t1.clone(), t2.clone(), JoinType::Inner, cond.clone());
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                build_columns_catalog("t1", vec!["a1", "b1", "c1"], false),
                build_columns_catalog("t2", vec!["a2", "b1", "c2"], false),
            ]
            .concat()
        );

        let plan = LogicalJoin::new(t1.clone(), t2.clone(), JoinType::Left, cond.clone());
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                build_columns_catalog("t1", vec!["a1", "b1", "c1"], false),
                build_columns_catalog("t2", vec!["a2", "b1", "c2"], true),
            ]
            .concat()
        );

        let plan = LogicalJoin::new(t1.clone(), t2.clone(), JoinType::Right, cond.clone());
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                build_columns_catalog("t1", vec!["a1", "b1", "c1"], true),
                build_columns_catalog("t2", vec!["a2", "b1", "c2"], false),
            ]
            .concat()
        );

        let plan = LogicalJoin::new(t1, t2, JoinType::Full, cond);
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                build_columns_catalog("t1", vec!["a1", "b1", "c1"], true),
                build_columns_catalog("t2", vec!["a2", "b1", "c2"], true),
            ]
            .concat()
        );
    }

    #[test]
    fn test_join_output_schema_when_three_tables() {
        let t1 = Arc::new(LogicalTableScan::new(
            "t1".to_string(),
            None,
            build_columns_catalog("t1", vec!["a1", "b1", "c1"], false),
            None,
            None,
        ));
        let t2 = Arc::new(LogicalTableScan::new(
            "t2".to_string(),
            None,
            build_columns_catalog("t2", vec!["a2", "b1", "c2"], false),
            None,
            None,
        ));
        let t3 = Arc::new(LogicalTableScan::new(
            "t3".to_string(),
            None,
            build_columns_catalog("t3", vec!["a3", "b3", "c1"], false),
            None,
            None,
        ));
        let cond1 = build_join_condition_eq("t1", "b1", "t2", "b1");
        let cond2 = build_join_condition_eq("t1", "c1", "t3", "c1");

        // inner join + inner join
        let plan = LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                t1.clone(),
                t2.clone(),
                JoinType::Inner,
                cond1.clone(),
            )),
            t3.clone(),
            JoinType::Inner,
            cond2.clone(),
        );
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                vec![
                    build_columns_catalog("t1", vec!["a1", "b1", "c1"], false),
                    build_columns_catalog("t2", vec!["a2", "b1", "c2"], false)
                ]
                .concat(),
                build_columns_catalog("t3", vec!["a3", "b3", "c1"], false),
            ]
            .concat()
        );
        // inner join + left join
        let plan = LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                t1.clone(),
                t2.clone(),
                JoinType::Inner,
                cond1.clone(),
            )),
            t3.clone(),
            JoinType::Left,
            cond2.clone(),
        );
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                vec![
                    build_columns_catalog("t1", vec!["a1", "b1", "c1"], false),
                    build_columns_catalog("t2", vec!["a2", "b1", "c2"], false)
                ]
                .concat(),
                build_columns_catalog("t3", vec!["a3", "b3", "c1"], true),
            ]
            .concat()
        );
        // inner join + right join
        let plan = LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                t1.clone(),
                t2.clone(),
                JoinType::Inner,
                cond1.clone(),
            )),
            t3.clone(),
            JoinType::Right,
            cond2.clone(),
        );
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                vec![
                    build_columns_catalog("t1", vec!["a1", "b1", "c1"], true),
                    build_columns_catalog("t2", vec!["a2", "b1", "c2"], true)
                ]
                .concat(),
                build_columns_catalog("t3", vec!["a3", "b3", "c1"], false),
            ]
            .concat()
        );
        // inner join + full join
        let plan = LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                t1.clone(),
                t2.clone(),
                JoinType::Inner,
                cond1.clone(),
            )),
            t3.clone(),
            JoinType::Full,
            cond2.clone(),
        );
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                vec![
                    build_columns_catalog("t1", vec!["a1", "b1", "c1"], true),
                    build_columns_catalog("t2", vec!["a2", "b1", "c2"], true)
                ]
                .concat(),
                build_columns_catalog("t3", vec!["a3", "b3", "c1"], true),
            ]
            .concat()
        );

        // left join + inner join
        let plan = LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                t1.clone(),
                t2.clone(),
                JoinType::Left,
                cond1.clone(),
            )),
            t3.clone(),
            JoinType::Inner,
            cond2.clone(),
        );
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                vec![
                    build_columns_catalog("t1", vec!["a1", "b1", "c1"], false),
                    build_columns_catalog("t2", vec!["a2", "b1", "c2"], true)
                ]
                .concat(),
                build_columns_catalog("t3", vec!["a3", "b3", "c1"], false),
            ]
            .concat()
        );
        // left join + left join
        let plan = LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                t1.clone(),
                t2.clone(),
                JoinType::Left,
                cond1.clone(),
            )),
            t3.clone(),
            JoinType::Left,
            cond2.clone(),
        );
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                vec![
                    build_columns_catalog("t1", vec!["a1", "b1", "c1"], false),
                    build_columns_catalog("t2", vec!["a2", "b1", "c2"], true)
                ]
                .concat(),
                build_columns_catalog("t3", vec!["a3", "b3", "c1"], true),
            ]
            .concat()
        );
        // left join + right join
        let plan = LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                t1.clone(),
                t2.clone(),
                JoinType::Left,
                cond1.clone(),
            )),
            t3.clone(),
            JoinType::Right,
            cond2.clone(),
        );
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                vec![
                    build_columns_catalog("t1", vec!["a1", "b1", "c1"], true),
                    build_columns_catalog("t2", vec!["a2", "b1", "c2"], true)
                ]
                .concat(),
                build_columns_catalog("t3", vec!["a3", "b3", "c1"], false),
            ]
            .concat()
        );
        // left join + full join
        let plan = LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                t1.clone(),
                t2.clone(),
                JoinType::Left,
                cond1.clone(),
            )),
            t3.clone(),
            JoinType::Full,
            cond2.clone(),
        );
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                vec![
                    build_columns_catalog("t1", vec!["a1", "b1", "c1"], true),
                    build_columns_catalog("t2", vec!["a2", "b1", "c2"], true)
                ]
                .concat(),
                build_columns_catalog("t3", vec!["a3", "b3", "c1"], true),
            ]
            .concat()
        );

        // right join + inner join
        let plan = LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                t1.clone(),
                t2.clone(),
                JoinType::Right,
                cond1.clone(),
            )),
            t3.clone(),
            JoinType::Inner,
            cond2.clone(),
        );
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                vec![
                    build_columns_catalog("t1", vec!["a1", "b1", "c1"], true),
                    build_columns_catalog("t2", vec!["a2", "b1", "c2"], false)
                ]
                .concat(),
                build_columns_catalog("t3", vec!["a3", "b3", "c1"], false),
            ]
            .concat()
        );
        // right join + left join
        let plan = LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                t1.clone(),
                t2.clone(),
                JoinType::Right,
                cond1.clone(),
            )),
            t3.clone(),
            JoinType::Left,
            cond2.clone(),
        );
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                vec![
                    build_columns_catalog("t1", vec!["a1", "b1", "c1"], true),
                    build_columns_catalog("t2", vec!["a2", "b1", "c2"], false)
                ]
                .concat(),
                build_columns_catalog("t3", vec!["a3", "b3", "c1"], true),
            ]
            .concat()
        );
        // right join + right join
        let plan = LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                t1.clone(),
                t2.clone(),
                JoinType::Right,
                cond1.clone(),
            )),
            t3.clone(),
            JoinType::Right,
            cond2.clone(),
        );
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                vec![
                    build_columns_catalog("t1", vec!["a1", "b1", "c1"], true),
                    build_columns_catalog("t2", vec!["a2", "b1", "c2"], true)
                ]
                .concat(),
                build_columns_catalog("t3", vec!["a3", "b3", "c1"], false),
            ]
            .concat()
        );
        // right join + full join
        let plan = LogicalJoin::new(
            Arc::new(LogicalJoin::new(t1, t2, JoinType::Right, cond1)),
            t3,
            JoinType::Full,
            cond2,
        );
        assert_eq!(
            plan.join_output_columns_internal(),
            vec![
                vec![
                    build_columns_catalog("t1", vec!["a1", "b1", "c1"], true),
                    build_columns_catalog("t2", vec!["a2", "b1", "c2"], true)
                ]
                .concat(),
                build_columns_catalog("t3", vec!["a3", "b3", "c1"], true),
            ]
            .concat()
        );
    }
}
