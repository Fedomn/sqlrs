use std::sync::Arc;

use anyhow::Result;
use sqlrs::main_entry::{ClientContext, DatabaseInstance};
use sqlrs::{cli, Database};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let db = Database::new_on_csv();
    create_csv_table(&db, "employee")?;
    create_csv_table(&db, "department")?;
    create_csv_table(&db, "state")?;
    create_csv_table(&db, "t1")?;
    create_csv_table(&db, "t2")?;

    let dbv2 = Arc::new(DatabaseInstance::default());
    dbv2.initialize()?;
    let client_context = ClientContext::new(dbv2);
    cli::interactive(db, client_context).await?;

    Ok(())
}

fn create_csv_table(db: &Database, table_name: &str) -> Result<()> {
    let table_name = table_name.to_string();
    let filepath = format!("./tests/csv/{}.csv", table_name);
    db.create_csv_table(table_name, filepath)?;

    Ok(())
}
