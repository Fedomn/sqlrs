/// The common trait over all plan nodes. Used by optimizer framework which will treat all node as `dyn PlanNode`
pub trait PlanNode {}

impl dyn PlanNode {}
