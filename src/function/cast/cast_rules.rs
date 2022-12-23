use crate::types_v2::LogicalType;

pub struct CastRules;

impl CastRules {
    pub fn implicit_cast_cost(from: &LogicalType, to: &LogicalType) -> i32 {
        if from == to {
            0
        } else if LogicalType::can_implicit_cast(from, to) {
            1
        } else {
            -1
        }
    }
}
