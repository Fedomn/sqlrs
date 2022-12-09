use super::TypeError;

/// Sqlrs type conversion:
/// sqlparser::ast::DataType -> LogicalType -> arrow::datatypes::DataType
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LogicalType {
    Invalid,
    Boolean,
    Tinyint,
    UTinyint,
    Smallint,
    USmallint,
    Integer,
    UInteger,
    Bigint,
    UBigint,
    Float,
    Double,
    Varchar,
}

impl LogicalType {
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::Tinyint
                | LogicalType::UTinyint
                | LogicalType::Smallint
                | LogicalType::USmallint
                | LogicalType::Integer
                | LogicalType::UInteger
                | LogicalType::Bigint
                | LogicalType::UBigint
                | LogicalType::Float
                | LogicalType::Double
        )
    }

    pub fn max_logical_type(
        left: &LogicalType,
        right: &LogicalType,
    ) -> Result<LogicalType, TypeError> {
        if left == right {
            return Ok(left.clone());
        }
        if left.is_numeric() && right.is_numeric() {
            if LogicalType::can_implicit_cast(left, right) {
                return Ok(right.clone());
            } else if LogicalType::can_implicit_cast(right, left) {
                return Ok(left.clone());
            } else {
                return Err(TypeError::InternalError(format!(
                    "can not implicit cast {:?} to {:?}",
                    left, right
                )));
            }
        }
        Err(TypeError::InternalError(format!(
            "can not compare two types: {:?} and {:?}",
            left, right
        )))
    }

    pub fn can_implicit_cast(from: &LogicalType, to: &LogicalType) -> bool {
        if from == to {
            return true;
        }
        match from {
            LogicalType::Invalid => false,
            LogicalType::Boolean => false,
            LogicalType::Tinyint => matches!(
                to,
                LogicalType::Smallint
                    | LogicalType::Integer
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
            ),
            LogicalType::UTinyint => matches!(
                to,
                LogicalType::USmallint
                    | LogicalType::UInteger
                    | LogicalType::UBigint
                    | LogicalType::Smallint
                    | LogicalType::Integer
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
            ),
            LogicalType::Smallint => matches!(
                to,
                LogicalType::Integer
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
            ),
            LogicalType::USmallint => matches!(
                to,
                LogicalType::UInteger
                    | LogicalType::UBigint
                    | LogicalType::Integer
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
            ),
            LogicalType::Integer => matches!(
                to,
                LogicalType::Bigint | LogicalType::Float | LogicalType::Double
            ),
            LogicalType::UInteger => matches!(
                to,
                LogicalType::UBigint
                    | LogicalType::Bigint
                    | LogicalType::Float
                    | LogicalType::Double
            ),
            LogicalType::Bigint => matches!(to, LogicalType::Float | LogicalType::Double),
            LogicalType::UBigint => matches!(to, LogicalType::Float | LogicalType::Double),
            LogicalType::Float => matches!(to, LogicalType::Double),
            LogicalType::Double => false,
            LogicalType::Varchar => false,
        }
    }
}

/// sqlparser datatype to logical type
impl TryFrom<sqlparser::ast::DataType> for LogicalType {
    type Error = TypeError;

    fn try_from(value: sqlparser::ast::DataType) -> Result<Self, Self::Error> {
        match value {
            sqlparser::ast::DataType::Char(_)
            | sqlparser::ast::DataType::Varchar(_)
            | sqlparser::ast::DataType::Nvarchar(_)
            | sqlparser::ast::DataType::Text
            | sqlparser::ast::DataType::String => Ok(LogicalType::Varchar),
            sqlparser::ast::DataType::Float(_) => Ok(LogicalType::Float),
            sqlparser::ast::DataType::Double => Ok(LogicalType::Double),
            sqlparser::ast::DataType::TinyInt(_) => Ok(LogicalType::Tinyint),
            sqlparser::ast::DataType::UnsignedTinyInt(_) => Ok(LogicalType::UTinyint),
            sqlparser::ast::DataType::SmallInt(_) => Ok(LogicalType::Smallint),
            sqlparser::ast::DataType::UnsignedSmallInt(_) => Ok(LogicalType::USmallint),
            sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => {
                Ok(LogicalType::Integer)
            }
            sqlparser::ast::DataType::UnsignedInt(_)
            | sqlparser::ast::DataType::UnsignedInteger(_) => Ok(LogicalType::UInteger),
            sqlparser::ast::DataType::BigInt(_) => Ok(LogicalType::Bigint),
            sqlparser::ast::DataType::UnsignedBigInt(_) => Ok(LogicalType::UBigint),
            sqlparser::ast::DataType::Boolean => Ok(LogicalType::Boolean),
            other => Err(TypeError::NotImplementedSqlparserDataType(
                other.to_string(),
            )),
        }
    }
}

impl From<LogicalType> for arrow::datatypes::DataType {
    fn from(value: LogicalType) -> Self {
        use arrow::datatypes::DataType;
        match value {
            LogicalType::Invalid => panic!("invalid logical type"),
            LogicalType::Boolean => DataType::Boolean,
            LogicalType::Tinyint => DataType::Int8,
            LogicalType::UTinyint => DataType::UInt8,
            LogicalType::Smallint => DataType::Int16,
            LogicalType::USmallint => DataType::UInt16,
            LogicalType::Integer => DataType::Int32,
            LogicalType::UInteger => DataType::UInt32,
            LogicalType::Bigint => DataType::Int64,
            LogicalType::UBigint => DataType::UInt64,
            LogicalType::Float => DataType::Float32,
            LogicalType::Double => DataType::Float64,
            LogicalType::Varchar => DataType::Utf8,
        }
    }
}
