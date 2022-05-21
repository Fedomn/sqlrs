#![feature(generators, proc_macro_hygiene, stmt_expr_attributes)]
#![feature(generic_associated_types)]
use anyhow::Result;

use crate::storage::{CsvStorage, Storage, Table, Transaction};

mod catalog;
mod storage;

#[tokio::main]
async fn main() -> Result<()> {
    let id = "test".to_string();
    let filepath = "./tests/yellow_tripdata_2019-01.csv".to_string();
    let storage = CsvStorage::new();
    storage.create_table(id.clone(), filepath)?;
    let table = storage.get_table(id)?;
    let mut tx = table.read()?;

    let mut total_cnt = 0;
    loop {
        let batch = tx.next_batch()?;
        match batch {
            Some(batch) => total_cnt += batch.num_rows(),
            None => break,
        }
    }
    println!("total_cnt = {:?}", total_cnt);
    Ok(())
}
