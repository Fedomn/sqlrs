use anyhow::Result;
use sql_query_engine_rs::util::pretty_batches;
use sql_query_engine_rs::Database;

#[tokio::main]
async fn main() -> Result<()> {
    let db = Database::new_on_csv();
    let table_name = "employee".to_string();
    let filepath = "./tests/csv/employee.csv".to_string();
    db.create_csv_table(table_name, filepath)?;

    let output = db
        .run("select first_name from employee where last_name = 'Hopkins'")
        .await?;
    pretty_batches(&output);

    let output = db
        .run("select sum(salary+1), count(salary), max(salary) from employee where id > 1")
        .await?;
    pretty_batches(&output);

    Ok(())
}
