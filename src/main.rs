use anyhow::Result;
use sql_query_engine_rs::{cli, Database};

#[tokio::main]
async fn main() -> Result<()> {
    let db = Database::new_on_csv();
    let table_name = "employee".to_string();
    let filepath = "./tests/csv/employee.csv".to_string();
    db.create_csv_table(table_name, filepath)?;

    cli::interactive(db).await?;

    Ok(())
}
