use std::fs::File;

use anyhow::Result;
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
                    let output = db.run(sql.as_str()).await;
                    match output {
                        Ok(res) => pretty_batches(&res),
                        Err(err) => println!("Run Error: {}", err),
                    }
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
