use std::path::PathBuf;
use std::sync::Arc;
use std::thread::spawn;

use tokio::fs::File;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{MutexGuard, RwLock, RwLockReadGuard, mpsc};
use tokio::task::JoinHandle;
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
    UnknownColumns(Vec<String>),
    DuplicatedColumns(Vec<String>),
    UnknownTable(String),
    WrongNumberOfColumnsInOrder { expected: usize, got: usize },
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
            columns,
            contains_header,
        } => {
            let table_manager = schema_manager
                .get_table_manager(&target)
                .await
                .ok_or(QueryPlanError::UnknownTable(target.clone()))?;
            let schema = table_manager
                .read_schema()
                .await
                .ok_or(QueryPlanError::UnknownTable(target))?;

            let mut column_order: Vec<String> = Vec::new();
            let mut unknown_columns = Vec::new();
            let mut duplicated_columns = Vec::new();
            if let Some(columns) = columns {
                if schema.table.columns().len() != columns.len() {
                    return Err(QueryPlanError::WrongNumberOfColumnsInOrder {
                        expected: columns.len(),
                        got: schema.table.columns().len(),
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
                return Err(QueryPlanError::UnknownColumns(unknown_columns));
            }

            if !duplicated_columns.is_empty() {
                return Err(QueryPlanError::DuplicatedColumns(duplicated_columns));
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
