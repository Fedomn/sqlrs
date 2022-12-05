use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::catalog_v2::{Catalog, DataTable, DataTableInfo};
use crate::execution::{ExecutionContext, ExecutorError, PhysicalCreateTable};

#[derive(new)]
pub struct CreateTable {
    pub(crate) plan: PhysicalCreateTable,
}

impl CreateTable {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, context: Arc<ExecutionContext>) {
        let schema = self.plan.info.base.base.schema;
        let table = self.plan.info.base.table;
        let column_definitions = self.plan.info.base.columns;
        let data_table = DataTable::new(
            DataTableInfo::new(schema.clone(), table.clone()),
            column_definitions,
        );
        Catalog::create_table(
            context.clone_client_context(),
            schema,
            table.clone(),
            data_table,
        )?;
        let array = Arc::new(StringArray::from(vec![format!("CREATE TABLE {}", table)]));
        let fields = vec![Field::new("success", DataType::Utf8, false)];
        yield RecordBatch::try_new(
            SchemaRef::new(Schema::new_with_metadata(fields, HashMap::new())),
            vec![array],
        )?;
    }
}
