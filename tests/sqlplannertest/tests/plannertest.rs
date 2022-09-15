use std::path::Path;

use anyhow::Result;
use sqlplannertest_test::DatabaseWrapper;

fn main() -> Result<()> {
    sqlplannertest::planner_test_runner(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("planner"),
        || async { Ok(DatabaseWrapper::new("../csv/**/*.csv")) },
    )?;
    Ok(())
}
