use core::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};
use crate::binder::{JoinCondition, JoinType};
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct PhysicalHashJoin {
    left: PlanRef,
    right: PlanRef,
    join_type: JoinType,
    join_condition: JoinCondition,
}

impl PhysicalHashJoin {
    pub fn new(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        join_condition: JoinCondition,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            join_condition,
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
}

impl PlanNode for PhysicalHashJoin {
    /// To handle multiple join conditions, such as:
    /// `select * from a left join b on a.id = b.id inner join c on b.id = c.id`
    ///
    /// The left child is: `a left join b`
    /// The right child is: `c`
    ///
    /// So in the left child schema, b's fields is nullable, therefore we should use left join
    /// schema directly, rather than set b's fields as non-nullable.
    fn schema(&self) -> Vec<ColumnCatalog> {
        let (left_join_keys_force_nullable, right_join_keys_force_nullable) = match self.join_type {
            JoinType::Inner => (false, false),
            JoinType::Left => (false, true),
            JoinType::Right => (true, false),
            JoinType::Full => (true, true),
            JoinType::Cross => unreachable!(""),
        };
        let left_fields = self
            .left
            .schema()
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
            .schema()
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

impl PlanTreeNode for PhysicalHashJoin {
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

impl fmt::Display for PhysicalHashJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalHashJoin: type {:?}, cond {:?}",
            self.join_type, self.join_condition
        )
    }
}

impl PartialEq for PhysicalHashJoin {
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
    use crate::optimizer::{LogicalTableScan, PhysicalTableScan};

    #[test]
    fn test_join_output_schema_when_two_tables() {
        let t1 = Arc::new(PhysicalTableScan::new(LogicalTableScan::new(
            "t1".to_string(),
            build_columns_catalog("t1", vec!["a1", "b1", "c1"], false),
        )));
        let t2 = Arc::new(PhysicalTableScan::new(LogicalTableScan::new(
            "t2".to_string(),
            build_columns_catalog("t2", vec!["a2", "b1", "c2"], false),
        )));
        let cond = build_join_condition_eq("t1", "b1", "t2", "b1");

        let plan = PhysicalHashJoin::new(t1.clone(), t2.clone(), JoinType::Inner, cond.clone());
        assert_eq!(
            plan.schema(),
            vec![
                build_columns_catalog("t1", vec!["a1", "b1", "c1"], false),
                build_columns_catalog("t2", vec!["a2", "b1", "c2"], false),
            ]
            .concat()
        );

        let plan = PhysicalHashJoin::new(t1.clone(), t2.clone(), JoinType::Left, cond.clone());
        assert_eq!(
            plan.schema(),
            vec![
                build_columns_catalog("t1", vec!["a1", "b1", "c1"], false),
                build_columns_catalog("t2", vec!["a2", "b1", "c2"], true),
            ]
            .concat()
        );

        let plan = PhysicalHashJoin::new(t1.clone(), t2.clone(), JoinType::Right, cond.clone());
        assert_eq!(
            plan.schema(),
            vec![
                build_columns_catalog("t1", vec!["a1", "b1", "c1"], true),
                build_columns_catalog("t2", vec!["a2", "b1", "c2"], false),
            ]
            .concat()
        );

        let plan = PhysicalHashJoin::new(t1, t2, JoinType::Full, cond);
        assert_eq!(
            plan.schema(),
            vec![
                build_columns_catalog("t1", vec!["a1", "b1", "c1"], true),
                build_columns_catalog("t2", vec!["a2", "b1", "c2"], true),
            ]
            .concat()
        );
    }

    #[test]
    fn test_join_output_schema_when_three_tables() {
        let t1 = Arc::new(PhysicalTableScan::new(LogicalTableScan::new(
            "t1".to_string(),
            build_columns_catalog("t1", vec!["a1", "b1", "c1"], false),
        )));
        let t2 = Arc::new(PhysicalTableScan::new(LogicalTableScan::new(
            "t2".to_string(),
            build_columns_catalog("t2", vec!["a2", "b1", "c2"], false),
        )));
        let t3 = Arc::new(PhysicalTableScan::new(LogicalTableScan::new(
            "t3".to_string(),
            build_columns_catalog("t3", vec!["a3", "b3", "c1"], false),
        )));
        let cond1 = build_join_condition_eq("t1", "b1", "t2", "b1");
        let cond2 = build_join_condition_eq("t1", "c1", "t3", "c1");

        // inner join + inner join
        let plan = PhysicalHashJoin::new(
            Arc::new(PhysicalHashJoin::new(
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
            plan.schema(),
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
        let plan = PhysicalHashJoin::new(
            Arc::new(PhysicalHashJoin::new(
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
            plan.schema(),
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
        let plan = PhysicalHashJoin::new(
            Arc::new(PhysicalHashJoin::new(
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
            plan.schema(),
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
        let plan = PhysicalHashJoin::new(
            Arc::new(PhysicalHashJoin::new(
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
            plan.schema(),
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
        let plan = PhysicalHashJoin::new(
            Arc::new(PhysicalHashJoin::new(
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
            plan.schema(),
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
        let plan = PhysicalHashJoin::new(
            Arc::new(PhysicalHashJoin::new(
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
            plan.schema(),
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
        let plan = PhysicalHashJoin::new(
            Arc::new(PhysicalHashJoin::new(
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
            plan.schema(),
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
        let plan = PhysicalHashJoin::new(
            Arc::new(PhysicalHashJoin::new(
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
            plan.schema(),
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
        let plan = PhysicalHashJoin::new(
            Arc::new(PhysicalHashJoin::new(
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
            plan.schema(),
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
        let plan = PhysicalHashJoin::new(
            Arc::new(PhysicalHashJoin::new(
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
            plan.schema(),
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
        let plan = PhysicalHashJoin::new(
            Arc::new(PhysicalHashJoin::new(
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
            plan.schema(),
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
        let plan = PhysicalHashJoin::new(
            Arc::new(PhysicalHashJoin::new(t1, t2, JoinType::Right, cond1)),
            t3,
            JoinType::Full,
            cond2,
        );
        assert_eq!(
            plan.schema(),
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
