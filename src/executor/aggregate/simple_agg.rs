use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema, SchemaRef};
use itertools::Itertools;

use super::create_accumulators;
use crate::binder::{BoundAggFunc, BoundExpr};
use crate::executor::*;
use crate::types::build_scalar_value_array;

pub struct SimpleAggExecutor {
    pub agg_funcs: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

impl SimpleAggExecutor {
    fn cast_agg_funcs(&self) -> Vec<BoundAggFunc> {
        self.agg_funcs
            .iter()
            .map(|e| match e {
                BoundExpr::AggFunc(agg) => agg.clone(),
                _ => unreachable!(""),
            })
            .collect_vec()
    }

    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self) {
        let mut accs = create_accumulators(&self.agg_funcs);
        let agg_funcs = self.cast_agg_funcs();

        let mut agg_fileds: Option<Vec<Field>> = None;

        #[for_await]
        for batch in self.child {
            let batch = batch?;
            // only support one epxrssion in aggregation, not supported example: `sum(distinct a)`
            let columns: Result<Vec<_>, ExecutorError> = agg_funcs
                .iter()
                .map(|agg| agg.exprs[0].eval_column(&batch))
                .try_collect();

            // build new schema for aggregation result
            if agg_fileds.is_none() {
                agg_fileds = Some(
                    self.agg_funcs
                        .iter()
                        .map(|agg| agg.eval_field(&batch))
                        .collect(),
                );
            }
            let columns = columns?;
            for (acc, column) in accs.iter_mut().zip_eq(columns.iter()) {
                acc.update_batch(column)?;
            }
        }

        let mut columns: Vec<ArrayRef> = Vec::new();
        for acc in accs.iter() {
            let res = acc.evaluate()?;
            columns.push(build_scalar_value_array(&res, 1));
        }

        let schema = SchemaRef::new(Schema::new(agg_fileds.unwrap()));
        yield RecordBatch::try_new(schema, columns)?;
    }
}
