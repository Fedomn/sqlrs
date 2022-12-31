use super::{PhysicalOperator, PhysicalOperatorBase};
use crate::catalog_v2::TableCatalogEntry;
use crate::execution::PhysicalPlanGenerator;
use crate::planner_v2::{BoundCreateTableInfo, LogicalInsert};
use crate::types_v2::LogicalType;

#[derive(Clone)]
pub struct PhysicalInsert {
    pub(crate) base: PhysicalOperatorBase,
    /// The insertion map ([table_index -> index in result, or INVALID_INDEX if not specified])
    pub(crate) column_index_list: Vec<usize>,
    /// The expected types for the INSERT statement
    pub(crate) expected_types: Vec<LogicalType>,
    /// The table to insert into, the table is none when create table as
    pub(crate) table: Option<TableCatalogEntry>,
    /// For create table as statement
    pub(crate) create_table_info: Option<BoundCreateTableInfo>,
}

impl PhysicalInsert {
    pub fn clone_with_base(&self, base: PhysicalOperatorBase) -> Self {
        Self {
            base,
            column_index_list: self.column_index_list.clone(),
            expected_types: self.expected_types.clone(),
            table: self.table.clone(),
            create_table_info: self.create_table_info.clone(),
        }
    }

    pub fn new_insert_into(
        base: PhysicalOperatorBase,
        column_index_list: Vec<usize>,
        expected_types: Vec<LogicalType>,
        table: TableCatalogEntry,
    ) -> Self {
        Self {
            base,
            column_index_list,
            expected_types,
            table: Some(table),
            create_table_info: None,
        }
    }

    pub fn new_create_table_as(
        base: PhysicalOperatorBase,
        create_table_info: BoundCreateTableInfo,
    ) -> Self {
        Self {
            base,
            column_index_list: vec![],
            expected_types: vec![],
            table: None,
            create_table_info: Some(create_table_info),
        }
    }
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_insert(&self, op: LogicalInsert) -> PhysicalOperator {
        let base = self.create_physical_operator_base(op.base);
        PhysicalOperator::PhysicalInsert(Box::new(PhysicalInsert::new_insert_into(
            base,
            op.column_index_list,
            op.expected_types,
            op.table,
        )))
    }
}
