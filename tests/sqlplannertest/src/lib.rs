#![feature(iterator_try_collect)]

use std::sync::Arc;

use anyhow::Error;
use sqlplannertest::{ParsedTestCase, PlannerTestRunner};
use sqlrs::db::Database;

fn init_tables(db: Arc<Database>, csv_path: &str) {
    let csv_files = glob::glob(csv_path).expect("failed to find csv files");
    for csv_file in csv_files {
        let filepath = csv_file.expect("failed to read csv file");
        let filename = filepath.file_stem().expect("failed to get file name");
        let filepath = filepath.to_str().unwrap();
        let filename = filename.to_str().unwrap();
        println!("create table {} from {}", filename, filepath);
        db.create_csv_table(filename.into(), filepath.into())
            .expect("failed to create table");
    }
}

pub struct DatabaseWrapper {
    db: Arc<Database>,
}

impl DatabaseWrapper {
    pub fn new(csv_path: &str) -> Self {
        let db = Arc::new(Database::new_on_csv());
        init_tables(db.clone(), csv_path);
        Self { db }
    }
}

#[async_trait::async_trait]
impl PlannerTestRunner for DatabaseWrapper {
    async fn run(&mut self, test_case: &ParsedTestCase) -> Result<String, Error> {
        let explain_str = self.db.explain(&test_case.sql).await?;
        Ok(explain_str)
    }
}
