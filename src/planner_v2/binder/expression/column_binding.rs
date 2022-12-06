use derive_new::new;

#[derive(new, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnBinding {
    pub(crate) table_idx: usize,
    pub(crate) column_idx: usize,
}
