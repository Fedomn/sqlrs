mod input_ref_rewrite;
mod physical_rewrite;
mod pushdown_limit;
mod pushdown_predicates;
use std::fmt::Debug;

use enum_dispatch::enum_dispatch;
pub use input_ref_rewrite::*;
pub use physical_rewrite::*;
pub use pushdown_limit::*;
pub use pushdown_predicates::*;
use strum_macros::AsRefStr;

use crate::optimizer::core::{OptExpr, Pattern, Rule, Substitute};

#[enum_dispatch(Rule)]
#[derive(Clone, AsRefStr)]
pub enum RuleImpl {
    InputRefRwriteRule,
    PhysicalRewriteRule,
    PushPredicateThroughJoin,
    LimitProjectTranspose,
    EliminateLimits,
    PushLimitThroughJoin,
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
        catalog
    }

    pub fn build_plan(sql: &str) -> PlanRef {
        let stats = parse(sql).unwrap();

        let catalog = new_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let bound_stmt = binder.bind(&stats[0]).unwrap();

        let planner = Planner {};
        planner.plan(bound_stmt).unwrap()
    }
}
