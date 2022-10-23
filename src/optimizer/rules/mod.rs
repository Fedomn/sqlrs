mod column_pruning;
mod combine_operators;
mod physical_rewrite;
mod pushdown_limit;
mod pushdown_predicates;
mod simplification;
mod util;
use std::fmt::Debug;

pub use column_pruning::*;
pub use combine_operators::*;
use enum_dispatch::enum_dispatch;
pub use physical_rewrite::*;
pub use pushdown_limit::*;
pub use pushdown_predicates::*;
pub use simplification::*;
use strum_macros::AsRefStr;

use crate::optimizer::core::{OptExpr, Pattern, Rule, Substitute};

#[enum_dispatch(Rule)]
#[derive(Clone, AsRefStr)]
pub enum RuleImpl {
    // Predicate pushdown
    PushPredicateThroughNonJoin,
    PushPredicateThroughJoin,
    // Limit pushdown
    LimitProjectTranspose,
    PushLimitThroughJoin,
    PushLimitIntoTableScan,
    EliminateLimits,
    // Column pruning
    PushProjectThroughChild,
    PushProjectIntoTableScan,
    RemoveNoopOperators,
    // Combine operators
    CollapseProject,
    CombineFilter,
    // Simplification
    SimplifyCasts,
    // Rewrite physical plan
    PhysicalRewriteRule,
}

impl Debug for RuleImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

#[cfg(test)]
mod rule_test_util {
    use std::sync::Arc;

    use crate::binder::test_util::build_table_catalog;
    use crate::binder::Binder;
    use crate::catalog::RootCatalog;
    use crate::optimizer::PlanRef;
    use crate::parser::parse;
    use crate::planner::Planner;

    pub struct RuleTest {
        pub name: &'static str,
        pub sql: &'static str,
        pub expect: &'static str,
    }

    fn new_catalog() -> RootCatalog {
        let mut catalog = RootCatalog::new();
        let t1 = "t1".to_string();
        let t1_catalog = build_table_catalog(t1.as_str(), vec!["a", "b", "c"]);
        catalog.tables.insert(t1, t1_catalog);
        let t2 = "t2".to_string();
        let t2_catalog = build_table_catalog(t2.as_str(), vec!["a", "b", "c"]);
        catalog.tables.insert(t2, t2_catalog);
        let employee = "employee".to_string();
        let employee_catalog = build_table_catalog(
            employee.as_str(),
            vec![
                "id",
                "first_name",
                "last_name",
                "state",
                "job_title",
                "salary",
                "department_id",
            ],
        );
        catalog.tables.insert(employee, employee_catalog);
        let department = "department".to_string();
        let department_catalog =
            build_table_catalog(department.as_str(), vec!["id", "department_name"]);
        catalog.tables.insert(department, department_catalog);
        let state = "state".to_string();
        let state_catalog =
            build_table_catalog(state.as_str(), vec!["id", "state_code", "state_name"]);
        catalog.tables.insert(state, state_catalog);
        catalog
    }

    pub fn build_plan(sql: &str) -> PlanRef {
        let stats = parse(sql).unwrap();

        let catalog = new_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let bound_stmt = binder.bind(&stats[0]).unwrap();

        let mut planner = Planner::default();
        planner.plan(bound_stmt).unwrap()
    }
}
