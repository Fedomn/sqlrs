mod errors;
mod table;

pub use errors::*;
pub use table::*;

#[derive(Debug, Clone)]
pub enum FunctionData {
    SeqTableScanInputData(Box<SeqTableScanInputData>),
    None,
}
