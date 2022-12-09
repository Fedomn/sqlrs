use std::env;
use std::fs::File;
use std::sync::Arc;

use anyhow::{Error, Result};
use rustyline::error::ReadlineError;
use rustyline::Editor;

use crate::main_entry::ClientContext;
use crate::util::pretty_batches;
use crate::Database;

pub async fn interactive(db: Database, client_context: Arc<ClientContext>) -> Result<()> {
    let mut rl = Editor::<()>::new()?;
    load_history(&mut rl);

    let mut enable_v2 = env::var("ENABLE_V2").unwrap_or_else(|_| "0".to_string()) == "1";

    loop {
        let read_sql = read_sql(&mut rl);
        match read_sql {
            Ok(sql) => {
                if !sql.trim().is_empty() {
                    rl.add_history_entry(sql.as_str());
                    let start_time = std::time::Instant::now();

                    if sql.starts_with("enable_v2") {
                        enable_v2 = true;
                        println!("---- enable sqlrs v2 ! ----");
                        continue;
                    }

                    if enable_v2 {
                        match client_context.query(sql).await {
                            Ok(_) => {}
                            Err(err) => println!("Run Error: {}", err),
                        }
                    } else {
                        run_sql(&db, sql).await?;
                    }

                    let end_time = std::time::Instant::now();
                    let time_consumed = end_time.duration_since(start_time);
                    println!("time consumed: {:?}", time_consumed);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted");
            }
            Err(ReadlineError::Eof) => {
                println!("Exited");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    save_history(&mut rl);
    Ok(())
}

fn load_history(rl: &mut Editor<()>) {
    let path = dirs::cache_dir().map(|p| {
        let cache_dir = p.join("sqlqueryenginers");
        std::fs::create_dir_all(cache_dir.as_path()).ok();
        let history_path = cache_dir.join("history.txt");
        if !history_path.as_path().exists() {
            File::create(history_path.as_path()).ok();
        }
        history_path.into_boxed_path()
    });

    if let Some(ref path) = path {
        if rl.load_history(path).is_err() {
            println!("No previous history.");
        }
    }
}

fn save_history(rl: &mut Editor<()>) {
    let path = dirs::cache_dir().map(|p| {
        let cache_dir = p.join("sqlqueryenginers");
        let history_path = cache_dir.join("history.txt");
        history_path.into_boxed_path()
    });

    if let Some(ref path) = path {
        if let Err(err) = rl.save_history(path) {
            println!("Save history failed {}.", err);
        }
    }
}

fn read_sql(rl: &mut Editor<()>) -> Result<String, ReadlineError> {
    let mut sql = String::new();
    loop {
        let prompt = if sql.is_empty() { "> " } else { "? " };
        let line = rl.readline(prompt)?;
        if line.is_empty() {
            continue;
        }

        // internal commands starts with "\"
        if line.starts_with('\\') && sql.is_empty() {
            return Ok(line);
        }

        sql.push_str(line.as_str());
        if line.ends_with(';') {
            return Ok(sql);
        } else {
            sql.push('\n');
        }
    }
}

async fn run_sql(db: &Database, sql: String) -> Result<()> {
    if let Some(cmds) = sql.trim().strip_prefix('\\') {
        match run_internal(db, cmds).await {
            Ok(_) => println!("Run Internal {} Success", cmds),
            Err(err) => println!("Run Internal {} Err: {}", cmds, err),
        }
        return Ok(());
    }

    match db.run(sql.as_str()).await {
        Ok(res) => pretty_batches(&res),
        Err(err) => println!("Run Error: {}", err),
    }
    Ok(())
}

async fn run_internal(db: &Database, cmds: &str) -> Result<()> {
    if cmds.starts_with("load csv") {
        if let Some((table_name, filepath)) = cmds.trim_start_matches("load csv ").split_once(' ') {
            load_csv(db, table_name.trim(), filepath.trim())
        } else {
            Err(Error::msg("Incorrect load csv command"))
        }
    } else if cmds.starts_with("dt") {
        show_tables(db)
    } else if cmds.starts_with("explain") {
        explain(db, cmds.trim_start_matches("explain ")).await
    } else {
        Err(Error::msg("Unknown internal command"))
    }
}

fn load_csv(db: &Database, table_name: &str, filepath: &str) -> Result<()> {
    println!("load csv {} {}", table_name, filepath);
    db.create_csv_table(table_name.to_string(), filepath.to_string())?;
    Ok(())
}

fn show_tables(db: &Database) -> Result<()> {
    let data = db.show_tables()?;
    pretty_batches(&vec![data]);
    Ok(())
}

async fn explain(db: &Database, sql: &str) -> Result<()> {
    let explain_str = db.explain(sql).await?;
    println!("\nexplain result for: {}\n\n{}", sql, explain_str);
    Ok(())
}
