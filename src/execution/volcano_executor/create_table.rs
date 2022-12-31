use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::catalog_v2::{Catalog, DataTable, DataTableInfo};
use crate::execution::{ExecutionContext, ExecutorError, PhysicalCreateTable};
use crate::planner_v2::BoundCreateTableInfo;
use crate::storage_v2::LocalStorage;

#[derive(new)]
pub struct CreateTable {
    pub(crate) plan: PhysicalCreateTable,
}

impl CreateTable {
    pub fn create_table(
        context: Arc<ExecutionContext>,
        info: &BoundCreateTableInfo,
    ) -> Result<DataTable, ExecutorError> {
        let schema = info.base.base.schema.clone();
        let table = info.base.table.clone();
        let column_definitions = info.base.columns.clone();
        let data_table = DataTable::new(
            DataTableInfo::new(schema.clone(), table.clone()),
            column_definitions,
        );
        Catalog::create_table(
            context.clone_client_context(),
            schema,
            table,
            data_table.clone(),
        )?;
        LocalStorage::init_table(context.clone_client_context(), &data_table);
        Ok(data_table)
    }

    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, context: Arc<ExecutionContext>) {
        let table = self.plan.info.base.table.clone();
        Self::create_table(context, &self.plan.info)?;
        let array = Arc::new(StringArray::from(vec![format!("CREATE TABLE {}", table)]));
        let fields = vec![Field::new("success", DataType::Utf8, false)];
        yield RecordBatch::try_new(
            SchemaRef::new(Schema::new_with_metadata(fields, HashMap::new())),
            vec![array],
        )?;
    }
}
