use anyhow::Result;
use sql_query_engine_rs::{cli, Database};

#[tokio::main]
async fn main() -> Result<()> {
    let db = Database::new_on_csv();
    create_csv_table(&db, "employee")?;
    create_csv_table(&db, "department")?;
    create_csv_table(&db, "state")?;
    create_csv_table(&db, "t1")?;
    create_csv_table(&db, "t2")?;

    cli::interactive(db).await?;

    Ok(())
}

fn create_csv_table(db: &Database, table_name: &str) -> Result<()> {
    let table_name = table_name.to_string();
    let filepath = format!("./tests/csv/{}.csv", table_name);
    db.create_csv_table(table_name, filepath)?;

    Ok(())
}
