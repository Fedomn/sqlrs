use strum_macros::AsRefStr;

use super::TypeError;

/// Sqlrs type conversion:
/// sqlparser::ast::DataType -> LogicalType -> arrow::datatypes::DataType
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, AsRefStr)]
pub enum LogicalType {
    Invalid,
    SqlNull,
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
    pub fn numeric() -> Vec<LogicalType> {
        vec![
            LogicalType::Tinyint,
            LogicalType::UTinyint,
            LogicalType::Smallint,
            LogicalType::USmallint,
            LogicalType::Integer,
            LogicalType::UInteger,
            LogicalType::Bigint,
            LogicalType::UBigint,
            LogicalType::Float,
            LogicalType::Double,
        ]
    }

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

    pub fn is_signed_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::Tinyint
                | LogicalType::Smallint
                | LogicalType::Integer
                | LogicalType::Bigint
        )
    }

    pub fn is_unsigned_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::UTinyint
                | LogicalType::USmallint
                | LogicalType::UInteger
                | LogicalType::UBigint
        )
    }

    pub fn max_logical_type(
        left: &LogicalType,
        right: &LogicalType,
    ) -> Result<LogicalType, TypeError> {
        if left == right {
            return Ok(left.clone());
        }
        match (left, right) {
            // SqlNull type can be cast to anything
            (LogicalType::SqlNull, _) => return Ok(right.clone()),
            (_, LogicalType::SqlNull) => return Ok(left.clone()),
            _ => {}
        }
        if left.is_numeric() && right.is_numeric() {
            return LogicalType::combine_numeric_types(left, right);
        }
        Err(TypeError::InternalError(format!(
            "can not compare two types: {:?} and {:?}",
            left, right
        )))
    }

    fn combine_numeric_types(
        left: &LogicalType,
        right: &LogicalType,
    ) -> Result<LogicalType, TypeError> {
        if left == right {
            return Ok(left.clone());
        }
        if left.is_signed_numeric() && right.is_unsigned_numeric() {
            // this method is symmetric
            // arrange it so the left type is smaller
            // to limit the number of options we need to check
            return LogicalType::combine_numeric_types(right, left);
        }

        if LogicalType::can_implicit_cast(left, right) {
            return Ok(right.clone());
        }
        if LogicalType::can_implicit_cast(right, left) {
            return Ok(left.clone());
        }
        // we can't cast implicitly either way and types are not equal
        // this happens when left is signed and right is unsigned
        // e.g. INTEGER and UINTEGER
        // in this case we need to upcast to make sure the types fit
        match (left, right) {
            (LogicalType::Bigint, _) | (_, LogicalType::UBigint) => Ok(LogicalType::Double),
            (LogicalType::Integer, _) | (_, LogicalType::UInteger) => Ok(LogicalType::Bigint),
            (LogicalType::Smallint, _) | (_, LogicalType::USmallint) => Ok(LogicalType::Integer),
            (LogicalType::Tinyint, _) | (_, LogicalType::UTinyint) => Ok(LogicalType::Smallint),
            _ => Err(TypeError::InternalError(format!(
                "can not combine these numeric types {:?} and {:?}",
                left, right
            ))),
        }
    }

    pub fn can_implicit_cast(from: &LogicalType, to: &LogicalType) -> bool {
        if from == to {
            return true;
        }
        match from {
            LogicalType::Invalid => false,
            LogicalType::SqlNull => true,
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
            LogicalType::SqlNull => DataType::Null,
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

impl TryFrom<&arrow::datatypes::DataType> for LogicalType {
    type Error = TypeError;

    fn try_from(value: &arrow::datatypes::DataType) -> Result<Self, Self::Error> {
        use arrow::datatypes::DataType;
        Ok(match value {
            DataType::Null => LogicalType::SqlNull,
            DataType::Boolean => LogicalType::Boolean,
            DataType::Int8 => LogicalType::Tinyint,
            DataType::Int16 => LogicalType::Smallint,
            DataType::Int32 => LogicalType::Integer,
            DataType::Int64 => LogicalType::Bigint,
            DataType::UInt8 => LogicalType::UTinyint,
            DataType::UInt16 => LogicalType::USmallint,
            DataType::UInt32 => LogicalType::UInteger,
            DataType::UInt64 => LogicalType::UBigint,
            DataType::Float16 => LogicalType::Float,
            DataType::Float32 => LogicalType::Float,
            DataType::Float64 => LogicalType::Double,
            DataType::Utf8 => LogicalType::Varchar,
            DataType::LargeUtf8 => LogicalType::Varchar,
            DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::List(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::Struct(_)
            | DataType::Union(_, _, _)
            | DataType::Dictionary(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Map(_, _) => {
                return Err(TypeError::NotImplementedArrowDataType(value.to_string()))
            }
        })
    }
}

impl std::fmt::Display for LogicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}
