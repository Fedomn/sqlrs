use std::path::Path;

use anyhow::Result;
use sqlplannertest_test::DatabaseWrapper;

#[tokio::main]
async fn main() -> Result<()> {
    sqlplannertest::planner_test_apply(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("planner"),
        || async { Ok(DatabaseWrapper::new("tests/csv/**/*.csv")) },
    )
    .await?;
    Ok(())
}
