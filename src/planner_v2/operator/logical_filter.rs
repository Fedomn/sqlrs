use super::LogicalOperatorBase;
use crate::function::ConjunctionType;
use crate::planner_v2::BoundExpression;

#[derive(Debug, Clone)]
pub struct LogicalFilter {
    pub(crate) base: LogicalOperatorBase,
}

impl LogicalFilter {
    fn split_predicates_internal(expr: BoundExpression) -> Vec<BoundExpression> {
        match expr {
            BoundExpression::BoundConjunctionExpression(e) => {
                if e.function.ty == ConjunctionType::And {
                    let mut res = vec![];
                    for child in e.children.into_iter() {
                        res.extend(Self::split_predicates_internal(child));
                    }
                    res
                } else {
                    vec![BoundExpression::BoundConjunctionExpression(e)]
                }
            }
            _ => vec![expr],
        }
    }

    // Split the predicates separated by AND statements
    // These are the predicates that are safe to push down because all of them MUST be true
    fn split_predicates(mut self) -> Self {
        let mut new_expressions = vec![];
        for expr in self.base.expressioins.into_iter() {
            let split_res = Self::split_predicates_internal(expr);
            new_expressions.extend(split_res);
        }
        self.base.expressioins = new_expressions;
        self
    }

    pub fn new(base: LogicalOperatorBase) -> Self {
        let op = Self { base };
        op.split_predicates()
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::function::{DefaultComparisonFunctions, DefaultConjunctionFunctions};
    use crate::planner_v2::{
        BindError, BoundColumnRefExpression, BoundComparisonExpression, BoundConjunctionExpression,
        BoundConstantExpression, BoundExpression, BoundExpressionBase, ColumnBinding,
    };
    use crate::types_v2::{LogicalType, ScalarValue};

    fn build_col_expr(name: String) -> BoundExpression {
        BoundExpression::BoundColumnRefExpression(BoundColumnRefExpression::new(
            BoundExpressionBase::new(name, LogicalType::Integer),
            ColumnBinding::new(1, 1),
            0,
        ))
    }

    fn build_eq_expr(
        left: BoundExpression,
        right: BoundExpression,
    ) -> Result<BoundExpression, BindError> {
        let eq_func = DefaultComparisonFunctions::get_comparison_function(
            &BinaryOperator::Eq,
            &LogicalType::Integer,
        )?;
        let base = BoundExpressionBase::new("".to_string(), LogicalType::Boolean);
        Ok(BoundExpression::BoundComparisonExpression(
            BoundComparisonExpression::new(base, Box::new(left), Box::new(right), eq_func),
        ))
    }

    fn build_and_expr(
        left: BoundExpression,
        right: BoundExpression,
    ) -> Result<BoundExpression, BindError> {
        let base = BoundExpressionBase::new("".to_string(), LogicalType::Boolean);
        let and_func = DefaultConjunctionFunctions::get_conjunction_function(&BinaryOperator::And)?;
        Ok(BoundExpression::BoundConjunctionExpression(
            BoundConjunctionExpression::new(base, and_func, vec![left, right]),
        ))
    }

    fn build_or_expr(
        left: BoundExpression,
        right: BoundExpression,
    ) -> Result<BoundExpression, BindError> {
        let base = BoundExpressionBase::new("".to_string(), LogicalType::Boolean);
        let and_func = DefaultConjunctionFunctions::get_conjunction_function(&BinaryOperator::Or)?;
        Ok(BoundExpression::BoundConjunctionExpression(
            BoundConjunctionExpression::new(base, and_func, vec![left, right]),
        ))
    }

    #[test]
    fn test_logical_filter_split_predicates() -> Result<(), BindError> {
        let v1 = BoundExpression::BoundConstantExpression(BoundConstantExpression::new(
            BoundExpressionBase::new("".to_string(), LogicalType::Integer),
            ScalarValue::Int32(Some(1)),
        ));
        let col1 = build_col_expr("col1".to_string());
        let col2 = build_col_expr("col2".to_string());
        let col3 = build_col_expr("col3".to_string());
        let col4 = build_col_expr("col4".to_string());
        let expr1 = build_eq_expr(col1, v1.clone())?;
        let expr2 = build_eq_expr(col2, v1.clone())?;
        let expr3 = build_eq_expr(col3, v1.clone())?;
        let expr4 = build_eq_expr(col4, v1)?;

        // And(And(Col1=1, Col2=1), And(Col3=1, Col4=1))
        let and_expr1 = build_and_expr(expr1.clone(), expr2.clone())?;
        let and_expr2 = build_and_expr(expr3.clone(), expr4.clone())?;
        let case1 = build_and_expr(and_expr1, and_expr2)?;
        let base = LogicalOperatorBase::new(vec![], vec![case1], vec![]);
        let op = LogicalFilter::new(base);
        assert_eq!(op.base.expressioins.len(), 4);

        // And(And(Col1=1, Col2=1), Or(Col3=1, Col4=1))
        let and_expr1 = build_and_expr(expr1, expr2)?;
        let and_expr2 = build_or_expr(expr3, expr4)?;
        let case2 = build_and_expr(and_expr1, and_expr2)?;
        let base = LogicalOperatorBase::new(vec![], vec![case2], vec![]);
        let op = LogicalFilter::new(base);
        assert_eq!(op.base.expressioins.len(), 3);

        Ok(())
    }
}
