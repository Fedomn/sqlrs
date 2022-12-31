use std::fs::File;
use std::sync::Arc;

use arrow::csv::{reader, Reader};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use derive_builder::Builder;
use futures::stream::BoxStream;
use sqlparser::ast::FunctionArg;

use super::{TableFunction, TableFunctionBindInput, TableFunctionInput};
use crate::function::{BuiltinFunctions, FunctionData, FunctionError, FunctionResult};
use crate::main_entry::ClientContext;
use crate::planner_v2::SqlparserResolver;
use crate::types_v2::LogicalType;

pub struct ReadCSV;

#[derive(Builder, Debug, Clone)]
pub struct ReadCSVInputData {
    pub(crate) filename: String,
    pub(crate) option: ReadCSVOptions,
    pub(crate) schema: SchemaRef,
    #[builder(default = "None")]
    pub(crate) bounds: Option<(usize, usize)>,
    #[builder(default = "None")]
    pub(crate) projection: Option<Vec<usize>>,
}

#[derive(Builder, Debug, Clone)]
pub struct ReadCSVOptions {
    #[builder(default = "1024")]
    pub(crate) infer_schema_max_rows: usize,
    #[builder(default = "1024")]
    pub(crate) read_batch_size: usize,
    #[builder(default = "true")]
    pub(crate) has_header: bool,
    #[builder(default = "b','")]
    pub(crate) delimiter: u8,
    #[builder(default = "None")]
    pub(crate) datetime_format: Option<String>,
}

impl ReadCSV {
    fn parse_filename(args: &[FunctionArg]) -> Result<String, FunctionError> {
        if let FunctionArg::Unnamed(e) = &args[0] {
            return Ok(SqlparserResolver::resolve_func_arg_expr_to_string(e)?);
        }
        Err(FunctionError::InternalError(format!(
            "unexpected filename arg: {}",
            &args[0]
        )))
    }

    fn parse_func_args(args: &[FunctionArg]) -> Result<(String, ReadCSVOptions), FunctionError> {
        if args.is_empty() {
            Err(FunctionError::InternalError(
                "filename is required".to_string(),
            ))
        } else {
            let filename = Self::parse_filename(args)?;
            let mut options = ReadCSVOptionsBuilder::default();
            for each in args.iter().skip(1) {
                if let FunctionArg::Named { name, arg } = each {
                    match name.value.as_str() {
                        "delim" => {
                            let string = SqlparserResolver::resolve_func_arg_expr_to_string(arg)?;
                            let bytes = string.as_bytes();
                            if bytes.len() != 1 {
                                return Err(FunctionError::InternalError(
                                    "delimiter must be a single byte".to_string(),
                                ));
                            }
                            options.delimiter(bytes[0]);
                        }
                        "header" => {
                            let v = SqlparserResolver::resolve_func_arg_expr_to_bool(arg)?;
                            options.has_header(v);
                        }
                        other => {
                            return Err(FunctionError::InternalError(format!(
                                "unexpected arg: {}",
                                other
                            )))
                        }
                    }
                } else {
                    return Err(FunctionError::InternalError(
                        "expected named arg".to_string(),
                    ));
                }
            }
            Ok((filename, options.build().unwrap()))
        }
    }

    fn infer_arrow_schema(
        filepath: String,
        option: &ReadCSVOptions,
    ) -> Result<SchemaRef, FunctionError> {
        let mut file = File::open(filepath)?;
        let (schema, _) = reader::infer_reader_schema(
            &mut file,
            option.delimiter,
            Some(option.infer_schema_max_rows),
            option.has_header,
        )?;
        Ok(Arc::new(schema))
    }

    fn create_reader(input: ReadCSVInputData) -> Result<Reader<File>, FunctionError> {
        let file = File::open(input.filename)?;
        // convert bounds into csv bounds concept: (min line, max line)
        let new_bounds = input.bounds.map(|(offset, limit)| {
            if limit == usize::MAX {
                (offset, limit)
            } else {
                (offset, offset + limit + 1)
            }
        });
        let reader = Reader::new(
            file,
            input.schema,
            input.option.has_header,
            Some(input.option.delimiter),
            input.option.read_batch_size,
            new_bounds,
            input.projection,
            input.option.datetime_format,
        );
        Ok(reader)
    }

    fn parse_col_names_types(
        schema: &SchemaRef,
    ) -> Result<(Vec<String>, Vec<LogicalType>), FunctionError> {
        let mut col_names = vec![];
        let mut col_types = vec![];
        for field in schema.fields() {
            col_names.push(field.name().to_string().to_lowercase());
            col_types.push(field.data_type().try_into()?);
        }
        Ok((col_names, col_types))
    }

    fn bind_func(
        _context: Arc<ClientContext>,
        input: TableFunctionBindInput,
        return_types: &mut Vec<LogicalType>,
        return_names: &mut Vec<String>,
    ) -> Result<Option<FunctionData>, FunctionError> {
        if let Some(args) = input.func_args {
            let (filename, option) = Self::parse_func_args(args.as_slice())?;
            let schema = Self::infer_arrow_schema(filename.clone(), &option)?;
            let (col_names, col_types) = Self::parse_col_names_types(&schema)?;
            return_types.extend(col_types);
            return_names.extend(col_names);
            let input_data = ReadCSVInputDataBuilder::default()
                .filename(filename)
                .schema(schema)
                .option(option)
                .build()
                .unwrap();
            Ok(Some(FunctionData::ReadCSVInputData(Box::new(input_data))))
        } else {
            Err(FunctionError::InternalError(
                "unexpected bind data type".to_string(),
            ))
        }
    }

    fn scan_func(
        _context: Arc<ClientContext>,
        input: TableFunctionInput,
    ) -> FunctionResult<BoxStream<'static, FunctionResult<RecordBatch>>> {
        if let Some(FunctionData::ReadCSVInputData(data)) = input.bind_data {
            let mut reader = Self::create_reader(*data)?;
            let stream = Box::pin(async_stream::try_stream! {
                while let Some(batch) = reader.next().transpose()? {
                    yield batch;
                }
            });
            Ok(stream)
        } else {
            Err(FunctionError::InternalError(
                "unexpected bind data type".to_string(),
            ))
        }
    }

    pub fn register_function(set: &mut BuiltinFunctions) -> Result<(), FunctionError> {
        set.add_table_functions(TableFunction::new(
            "read_csv".to_string(),
            Some(Self::bind_func),
            Self::scan_func,
        ))?;
        Ok(())
    }
}
