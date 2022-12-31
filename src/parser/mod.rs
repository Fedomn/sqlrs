use sqlparser::ast::{Query, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::{Parser, ParserError};

pub fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = PostgreSqlDialect {};
    let stmts = Parser::parse_sql(&dialect, sql)?;
    if stmts.is_empty() {
        return Err(ParserError::ParserError("empty string".to_string()));
    }
    Ok(stmts)
}

pub struct Sqlparser {}

impl Sqlparser {
    pub fn parse(sql: String) -> Result<Vec<Statement>, ParserError> {
        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, sql.as_str())?;
        Ok(stmts)
    }

    pub fn parse_one_stmt(sql: &str) -> Result<Statement, ParserError> {
        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, sql)?;
        if stmts.len() != 1 {
            return Err(ParserError::ParserError(
                "not a single statement".to_string(),
            ));
        }
        Ok(stmts[0].clone())
    }

    pub fn parse_one_query(sql: &str) -> Result<Box<Query>, ParserError> {
        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, sql)?;
        if stmts.len() != 1 {
            return Err(ParserError::ParserError(
                "not a single statement".to_string(),
            ));
        }
        match stmts[0].clone() {
            Statement::Query(q) => Ok(q),
            _ => Err(ParserError::ParserError(
                "only expect query statement".to_string(),
            )),
        }
    }
}
