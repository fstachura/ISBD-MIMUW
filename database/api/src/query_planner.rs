use std::path::PathBuf;
use std::sync::Arc;
use std::thread::spawn;

use tokio::fs::File;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{MutexGuard, RwLock, RwLockReadGuard, mpsc};
use tokio::task::JoinHandle;
use tracing::{Level, event};
use uuid::Uuid;

use crate::query_manager::{Query, QueryError, QueryManager, QueryStateMarker};
use crate::schema_manager::{LockedTable, SchemaManager, TableLockedForCopy, TableManager};
use schema::{Column, ColumnType};

// schema manager: stores information about tables, allows for locking tables
// query manager: stores information about pending queries
// query planner: gathers information from schema manager into a plan, may also lock tables
// query executor: executes queries

// assumption: single database process (i think it's a valid assumption here? i don't see a good
// reason for many database processes)
// schema: directory with json files. read into schema manager on startup. each table has a
// separate rwlock and copy mutex

// single schema architecture:

#[derive(Debug)]
pub enum QueryPlan {
    // mutexed copy void for each table. lock mutex, rlock schema, generate filenames. create and write files, rwlock schema, add files, release
    // rwlock and then mutex.
    // why mutex and not write? that way table can be read while data is copied.
    Copy {
        // locked table metadata file?
        source: PathBuf,
        column_order: Vec<String>,
        contains_header: bool,
        table: TableManager,
        // rlocked and copylocked tablemanager
    },
    // readlock schema, open all files, pass readlock and files to executor, get data, unreadlock schema.
    // unreadlocking before could cause race condition during delete except on linux (optional?)
    Select {
        columns: Vec<Column>,
        table: TableManager,
    },
}

#[derive(Clone, Debug)]
pub enum QueryPlanError {
    UnknownColumns {
        table_or_filename: String, 
        column_order_from_header: bool,
        columns: Vec<String>,
    },
    DuplicatedColumns{
        table_or_filename: String,
        column_order_from_header: bool,
        columns: Vec<String>,
    },
    UnknownTable(String),
    WrongNumberOfColumnsInOrder {
        table_or_filename: String,
        column_order_from_header: bool,
        expected: usize,
        got: usize,
    },
    FailedToParseCsv(Option<String>),
    FailedToOpenCsv(Option<String>),
    TooManyColumnsInCsvAndNoOrder(String, Option<String>),
    UnknownError,
}

pub async fn plan_query(
    schema_manager: &SchemaManager,
    query: Query,
) -> Result<QueryPlan, QueryPlanError> {
    match query {
        Query::Select { table } => {
            let table_manager = schema_manager
                .get_table_manager(&table)
                .await
                .ok_or(QueryPlanError::UnknownTable(table.clone()))?;
            let schema = table_manager
                .read_schema()
                .await
                .ok_or(QueryPlanError::UnknownTable(table))?;

            Ok(QueryPlan::Select {
                table: table_manager.clone(),
                columns: (*schema.table.columns()).clone(),
            })
        }
        Query::Copy {
            source,
            target,
            columns: mut requested_column_order,
            contains_header,
        } => {
            let table_manager = schema_manager
                .get_table_manager(&target)
                .await
                .ok_or(QueryPlanError::UnknownTable(target.clone()))?;
            let schema = table_manager
                .read_schema()
                .await
                .ok_or(QueryPlanError::UnknownTable(target.clone()))?;

            let table_name = schema.table.name().clone();
            let columns_num = schema.table.columns().len();
            let columns_is_none = requested_column_order.is_none();
            let source_tmp = source.clone();

            let csv_check_result = tokio::task::spawn_blocking(move || {
                let mut reader = csv::ReaderBuilder::new()
                    .delimiter(b';')
                    .has_headers(contains_header)
                    .from_path(source_tmp.clone())
                    .map_err(|e| {
                        QueryPlanError::FailedToOpenCsv(source_tmp.to_str().map(|v| v.to_string()))
                    })?;

                match reader.records().next() {
                    Some(Ok(record)) => {
                        if record.len() > columns_num && columns_is_none {
                            Err(QueryPlanError::TooManyColumnsInCsvAndNoOrder(
                                table_name,
                                source_tmp.to_str().map(|v| v.to_string()),
                            ))
                        } else if contains_header {
                            Ok(Some(record.iter().map(|v| v.to_string()).collect::<Vec<String>>()))
                        } else {
                            Ok(None)
                        }
                    }
                    err => {
                        event!(Level::ERROR, "failed to open csv file in planner {err:?}");
                        Err(QueryPlanError::FailedToParseCsv(
                            source_tmp.to_str().map(|v| v.to_string()),
                        ))
                    }
                }
            })
            .await;

            let mut error_target = target;
            let mut column_order_from_header = false;
            match csv_check_result {
                Err(err) => {
                    event!(Level::ERROR, "failed to join csv check task {err:?}");
                    return Err(QueryPlanError::UnknownError);
                },
                Ok(Err(err)) => {
                    return Err(err);
                },
                Ok(Ok(Some(header))) => {
                    if requested_column_order.is_none() {
                        requested_column_order = Some(header);
                        error_target = source.to_str().unwrap_or("failed to parse filename").to_string();
                        column_order_from_header = true;
                    }
                },
                Ok(Ok(None)) => {},
            }

            let mut column_order: Vec<String> = Vec::new();
            let mut unknown_columns = Vec::new();
            let mut duplicated_columns = Vec::new();
            if let Some(columns) = requested_column_order {
                if schema.table.columns().len() != columns.len() {
                    return Err(QueryPlanError::WrongNumberOfColumnsInOrder {
                        table_or_filename: error_target.clone(),
                        column_order_from_header,
                        expected: schema.table.columns().len(),
                        got: columns.len(),
                    });
                }

                for col in columns {
                    if let None = schema.table.columns().iter().find(|c| c.name == col) {
                        unknown_columns.push(col);
                    } else {
                        if let None = column_order.iter().find(|c| **c == col) {
                            column_order.push(col);
                        } else {
                            duplicated_columns.push(col);
                        }
                    }
                }
            } else {
                column_order = schema
                    .table
                    .columns()
                    .iter()
                    .map(|c| c.name.clone())
                    .collect();
            }

            if !unknown_columns.is_empty() {
                return Err(QueryPlanError::UnknownColumns {
                    table_or_filename: error_target.clone(),
                    columns: unknown_columns,
                    column_order_from_header,
                });
            }

            if !duplicated_columns.is_empty() {
                return Err(QueryPlanError::DuplicatedColumns {
                    table_or_filename: error_target.clone(),
                    columns: duplicated_columns,
                    column_order_from_header,
                });
            }

            Ok(QueryPlan::Copy {
                source: source.clone(),
                column_order,
                contains_header,
                table: table_manager.clone(),
            })
        }
    }
}
