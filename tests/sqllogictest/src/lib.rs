#![feature(iterator_try_collect)]

use std::sync::Arc;

use sqllogictest::{AsyncDB, Runner};
use sqlrs::db::{Database, DatabaseError};
use sqlrs::main_entry::{ClientContext, DatabaseError as DatabaseErrorV2, DatabaseInstance};
use sqlrs::util::record_batch_to_string;

fn init_tables(db: Arc<Database>) {
    const CSV_FILES: &str = "tests/csv/**/*.csv";

    let csv_files = glob::glob(CSV_FILES).expect("failed to find csv files");
    for csv_file in csv_files {
        let filepath = csv_file.expect("failed to read csv file");
        let filename = filepath.file_stem().expect("failed to get file name");
        let filepath = filepath.to_str().unwrap();
        let filename = filename.to_str().unwrap();
        db.create_csv_table(filename.into(), filepath.into())
            .expect("failed to create table");
    }
}

pub fn test_run(sqlfile: &str) {
    let db = Arc::new(Database::new_on_csv());
    init_tables(db.clone());
    println!("init database with csv tables done for {}", sqlfile);

    let mut tester = Runner::new(DatabaseWrapper { db });
    tester.run_file(sqlfile).unwrap()
}

struct DatabaseWrapper {
    db: Arc<Database>,
}

#[async_trait::async_trait]
impl AsyncDB for DatabaseWrapper {
    type Error = DatabaseError;
    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        let chunks = self.db.run(sql).await?;
        let output = chunks.iter().map(record_batch_to_string).try_collect();
        Ok(output?)
    }
}

struct DatabaseWrapperV2 {
    client_context: Arc<ClientContext>,
}

#[async_trait::async_trait]
impl AsyncDB for DatabaseWrapperV2 {
    type Error = DatabaseErrorV2;

    async fn run(&mut self, sql: &str) -> Result<String, Self::Error> {
        let chunks = self.client_context.query(sql.to_string()).await?;
        let output = chunks.iter().map(record_batch_to_string).try_collect();
        Ok(output?)
    }

    fn engine_name(&self) -> &str {
        "sqlrs_v2"
    }
}

pub fn test_run_v2(sqlfile: &str) {
    let dbv2 = Arc::new(DatabaseInstance::default());
    dbv2.initialize().unwrap();
    let client_context = ClientContext::new(dbv2);
    let mut tester = Runner::new(DatabaseWrapperV2 { client_context });
    tester.run_file(sqlfile).unwrap()
}
