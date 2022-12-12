use derive_new::new;

use super::LogicalOperatorBase;
use crate::planner_v2::BoundCreateTableInfo;

#[derive(new, Debug, Clone)]
pub struct LogicalCreateTable {
    #[new(default)]
    pub(crate) base: LogicalOperatorBase,
    pub(crate) info: BoundCreateTableInfo,
}
