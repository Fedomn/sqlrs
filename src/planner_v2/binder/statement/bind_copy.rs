use log::debug;
use sqlparser::ast::{CopyOption, CopyTarget, Ident, ObjectName, Statement};

use super::BoundStatement;
use crate::parser::Sqlparser;
use crate::planner_v2::{BindError, Binder, SqlparserResolver, LOGGING_TARGET};

impl Binder {
    /// convert copy from csv into insert statement from csv_read table function
    fn convert_copy_from_to_insert_sql(
        table_name: &ObjectName,
        columns: &[Ident],
        target: &CopyTarget,
        options: &[CopyOption],
    ) -> Result<String, BindError> {
        let (schema_name, table_name) = SqlparserResolver::object_name_to_schema_table(table_name)?;
        let col_names = columns
            .iter()
            .map(|c| c.to_string().to_lowercase())
            .collect::<Vec<_>>();

        let (insert_cols, read_cols) = if col_names.is_empty() {
            // insert into main.t1 select * from read_csv('file.csv');
            ("".to_string(), "*".to_string())
        } else {
            // insert into main.t1(v1) select * from read_csv('file.csv');
            (format!("({})", col_names.join(",")), col_names.join(","))
        };
        let insert_sql = format!("insert into {}.{}{}", schema_name, table_name, insert_cols,);

        let read_csv_sql = Self::build_read_csv_sql(target, options)?;
        let csv_read_sql = format!("select {} from {}", read_cols, read_csv_sql);

        Ok(format!("{} {}", insert_sql, csv_read_sql))
    }

    fn build_read_csv_sql(
        target: &CopyTarget,
        options: &[CopyOption],
    ) -> Result<String, BindError> {
        let filename = match target {
            CopyTarget::File { filename } => filename,
            _ => {
                return Err(BindError::UnsupportedStmt(format!(
                    "unsupported copy target {:?}",
                    target
                )))
            }
        };
        let options_strs = options
            .iter()
            .filter_map(|o| match o {
                CopyOption::Delimiter(v) => Some(format!("delim=>'{}'", v)),
                CopyOption::Header(v) => Some(format!("header=>{}", v)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let options_str = if options_strs.is_empty() {
            "".to_string()
        } else {
            format!(" ,{}", options_strs.join(", "))
        };
        Ok(format!("read_csv('{}'{})", filename, options_str))
    }

    pub fn bind_copy(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::Copy {
                table_name,
                columns,
                to,
                target,
                options,
                legacy_options: _,
                values: _,
            } => {
                if *to {
                    return Err(BindError::UnsupportedStmt(
                        "unsupported copy to statement".to_string(),
                    ));
                }

                let insert_from_sql =
                    Self::convert_copy_from_to_insert_sql(table_name, columns, target, options)?;
                debug!(
                    target: LOGGING_TARGET,
                    "Copy converted raw sql: {:?}", insert_from_sql
                );
                let stmt = Sqlparser::parse_one_stmt(&insert_from_sql)?;
                self.bind(&stmt)
            }
            _ => Err(BindError::UnsupportedStmt(format!("{:?}", stmt))),
        }
    }
}
