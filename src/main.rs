#![feature(generators, proc_macro_hygiene, stmt_expr_attributes)]
use anyhow::Result;
use datasource::{CsvConfig, CsvDataSource, Datasource};
use futures_async_stream::for_await;

mod datasource;

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = CsvConfig::default();
    let filename = "./tests/yellow_tripdata_2019-01.csv".to_string();
    let csv = CsvDataSource::new(filename, &cfg)?;
    let mut total_cnt = 0;
    let stream = csv.execute();
    #[for_await]
    for batch in stream {
        total_cnt += batch?.num_rows();
    }
    println!("total_cnt = {:?}", total_cnt);
    Ok(())
}
