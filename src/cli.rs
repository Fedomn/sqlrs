use std::fs::File;

use anyhow::{Error, Result};
use rustyline::error::ReadlineError;
use rustyline::Editor;

use crate::util::pretty_batches;
use crate::Database;

pub async fn interactive(db: Database) -> Result<()> {
    let mut rl = Editor::<()>::new()?;
    load_history(&mut rl);

    loop {
        let read_sql = read_sql(&mut rl);
        match read_sql {
            Ok(sql) => {
                if !sql.trim().is_empty() {
                    rl.add_history_entry(sql.as_str());
                    run_sql(&db, sql).await?;
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
        match run_internal(db, cmds) {
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

fn run_internal(db: &Database, cmds: &str) -> Result<()> {
    if cmds.starts_with("load csv") {
        if let Some((table_name, filepath)) = cmds.trim_start_matches("load csv ").split_once(' ') {
            load_csv(db, table_name.trim(), filepath.trim())
        } else {
            Err(Error::msg("Incorrect load csv command"))
        }
    } else {
        Err(Error::msg("Unknown internal command"))
    }
}

fn load_csv(db: &Database, table_name: &str, filepath: &str) -> Result<()> {
    println!("load csv {} {}", table_name, filepath);
    db.create_csv_table(table_name.to_string(), filepath.to_string())?;
    Ok(())
}
