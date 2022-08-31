use crate::optimizer::rules::RuleImpl;

#[derive(Clone, Copy)]
pub enum HepMatchOrder {
    /// Match from root down. A match attempt at an ancestor always precedes all match attempts at
    /// its descendants.
    TopDown,
    /// Match from leaves up. A match attempt at a descendant precedes all match attempts at its
    /// ancestors.
    BottomUp,
}

pub type HepMatchLimit = u32;

/// HepState is mutable state that is changed by instruction, and controls the rules applying phase.
#[derive(Clone)]
pub struct HepState {
    pub(super) match_order: HepMatchOrder,
    pub(super) match_limit: HepMatchLimit,
}

/// HepInstruction represents one instruction in a HepProgram.
#[derive(Clone)]
pub enum HepInstruction {
    Rule(RuleImpl),
    Rules(Vec<RuleImpl>),
    MatchOrder(HepMatchOrder),
    MatchLimit(HepMatchLimit),
}

/// HepProgram specifies the order in which rules should be attempted by HepPlanner.
#[derive(Clone)]
pub struct HepProgram {
    pub(super) instructions: Vec<HepInstruction>,
    pub(super) state: HepState,
}

impl HepProgram {
    pub fn new(instructions: Vec<HepInstruction>) -> HepProgram {
        HepProgram {
            instructions,
            state: HepState {
                match_order: HepMatchOrder::TopDown,
                match_limit: u32::MAX,
            },
        }
    }
}
