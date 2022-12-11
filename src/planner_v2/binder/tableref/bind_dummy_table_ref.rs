use derive_new::new;

use super::BoundTableRef;
use crate::planner_v2::{BindError, Binder};

#[derive(new, Debug)]
pub struct BoundDummyTableRef {
    pub(crate) bind_index: usize,
}

impl Binder {
    pub fn bind_dummy_table_ref(&mut self) -> Result<BoundTableRef, BindError> {
        let table_index = self.generate_table_index();
        let bound_tabel_ref =
            BoundTableRef::BoundDummyTableRef(BoundDummyTableRef::new(table_index));
        Ok(bound_tabel_ref)
    }
}
