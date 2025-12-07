use std::sync::{Arc};
use std::path::PathBuf;
use std::thread::spawn;
use std::iter::Enumerate;

use chrono::format::parse_and_remainder;
use tokio::io::AsyncReadExt;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{MutexGuard, RwLock, RwLockReadGuard, mpsc};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::{fs::File};
use uuid::Uuid;

use crate::query_manager::{self, Query, QueryError, QueryManager, QueryStateMarker};
use crate::schema_manager::{LockedTable, SchemaManager, TableLockedForCopy, TableManager};
use schema::{Column, ColumnType};
use crate::query_planner::{QueryPlan, plan_query};
use format::format::{CHUNK_HEADER_SIZE, ChunkHeader, DeserializerError, HEADER_SIZE, parse_chunk_header, parse_header, parse_int64_chunk, parse_str_chunk};

#[derive(Debug)]
enum ReadError {
    DeserializerError(String, DeserializerError),
    IoError(String, std::io::Error),
}

enum ReadResult {
    Int64(Vec<Vec<i64>>),
    Str(Vec<Vec<String>>),
}

pub async fn read_chunk_header(file: &mut File) -> Result<(u64, Vec<u8>), ReadError> {
    let mut chunk_buf: [u8; CHUNK_HEADER_SIZE] = [0; CHUNK_HEADER_SIZE];

    file.read_exact(&mut chunk_buf).await
        .map_err(|e| ReadError::IoError("failed to read chunk header".into(), e))?;
    let chunk_header = parse_chunk_header(&mut chunk_buf)
        .map_err(|e| ReadError::DeserializerError("failed to parse chunk".into(), e))?;

    let mut chunk_bytes = vec![0_u8; chunk_header.bytes as usize];
    file.read_exact(&mut chunk_bytes).await
        .map_err(|e| ReadError::IoError("failed to read chunk".into(), e))?;

    Ok((chunk_header.rows, chunk_bytes))
}

pub async fn read_file(mut file: File) -> Result<ReadResult, ReadError> {
    let mut buf: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
    file.read_exact(&mut buf).await
        .map_err(|e| ReadError::IoError("failed to read header".into(), e))?;
    let (column_type, chunks) = parse_header(&buf)
        .map_err(|e| ReadError::DeserializerError("failed to parse header".into(), e))?;

    if column_type == format::format::ColumnType::Int64 {
        let mut result: Vec<Vec<i64>> = vec![];

        for chunk in 0..chunks {
            let (rows, chunk_bytes) = read_chunk_header(&mut file).await?;

            let mut sl = chunk_bytes.as_slice();
            let it = parse_int64_chunk(rows, &mut sl)
                .map_err(|e| ReadError::DeserializerError("failed to parse first int".into(), e))?;

            let data: Result<Vec<i64>, DeserializerError> = it.collect();
            result.push(
                data.map_err(|e| ReadError::DeserializerError("failed to parse int chunk".into(), e))?
            );
        }

        Ok(ReadResult::Int64(result))
    } else if column_type == format::format::ColumnType::Str {
        let mut result: Vec<Vec<String>> = vec![];

        for chunk in 0..chunks {
            let (rows, chunk_bytes) = read_chunk_header(&mut file).await?;

            let mut sl = chunk_bytes.as_slice();
            let it = parse_str_chunk(rows, &mut sl)
                .map_err(|e| ReadError::DeserializerError("failed to parse first int".into(), e))?;

            let data: Result<Vec<String>, DeserializerError> = it.collect();
            result.push(
                data.map_err(|e| ReadError::DeserializerError("failed to parse string chunk".into(), e))?
            );
        }

        Ok(ReadResult::Str(result))
    } else {
        unreachable!("unknown column type");
    }
}

#[derive(Debug)]
enum ExecuteError {
    IoError(String, std::io::Error),
    ReadError(ReadError),
    UnknownError(String),
}

enum SelectResult {
    // file (assuming there aren't that many), chunk, data...
    Int64(Vec<Vec<Vec<i64>>>),
    Str(Vec<Vec<Vec<String>>>),
    Empty,
}

pub async fn execute_copy() -> Result<(), ExecuteError> {
    // * lock mutex
    //     * rlock table - delete will need to get write first
    //         * generate filenames
    //         * write files
    //         * upgrade lock to write - it may now turn out that the table got deleted
    //             * push data to table, delete files is table got deleted

    Ok(())
}

pub async fn execute_select(data_dir: &PathBuf, columns: &Vec<Column>) -> Result<Vec<SelectResult>, ExecuteError> {
    // read lock schema, get all files, for each file spawn a routine that reads the file, collect
    // data from routines into a vector

    let mut joins = JoinSet::new();

    for (i, col) in columns.iter().enumerate() {
        for (j, path) in col.get_file_paths(data_dir).iter().enumerate() {
            let mut file = File::options()
                .read(true)
                .write(false)
                .open(path).await;

            match file {
                Ok(file) => {
                    // TODO spawn_blocked?
                    joins.spawn(async move {
                        read_file(file).await.map(|data| (i, j, data))
                    });
                },
                Err(err) => {
                    return Err(ExecuteError::IoError(format!("failed to open file {:?}", path), err));
                }
            }
        }
    }

    let mut result_vec: Vec<Vec<(usize, ReadResult)>> = vec![];
    let mut last_nonjoin_error = None;
    while let Some(result) = joins.join_next().await {
        match result {
            Ok(Ok((col, chunk, data))) => {
                let col_vec = if let Some(v) = result_vec.get_mut(col) {
                    v
                } else {
                    assert!(col >= result_vec.len());
                    result_vec.resize_with(col+1, || vec![]);
                    result_vec.get_mut(col)
                        .expect("vector somehow too small after resizing")
                };

                col_vec.push((chunk, data));
            },
            Ok(Err(read_err)) => {
                println!("read task failed {:?}", read_err);
                last_nonjoin_error = Some(ExecuteError::ReadError(read_err));
                joins.abort_all();
            },
            Err(join_err) => {
                if join_err.is_panic() {
                    let msg = format!("read task panicked {:?}", join_err.into_panic());
                    println!("{}", msg.clone());
                    last_nonjoin_error = Some(ExecuteError::UnknownError(msg));
                }
                joins.abort_all();
            },
        }
    }

    if let Some(err) = last_nonjoin_error {
        Err(err)
    } else {
        let mut result = vec![];

        for (col_i, col_vec) in result_vec.iter_mut().enumerate() {
            col_vec.sort_by(|(a, _), (b, _)| a.cmp(b));
            let mut col_result = None;
            let mut mismatch = false;
            for file in col_vec.drain(..) {
                col_result = match (col_result, file.1) {
                    (Some(SelectResult::Int64(mut v)), ReadResult::Int64(a)) => {
                        v.push(a);
                        Some(SelectResult::Int64(v))
                    },
                    (None, ReadResult::Int64(a)) =>
                        Some(SelectResult::Int64(vec![a])),
                    (Some(SelectResult::Str(mut v)), ReadResult::Str(a)) => {
                        v.push(a);
                        Some(SelectResult::Str(v))
                    },
                    (None, ReadResult::Str(a)) =>
                        Some(SelectResult::Str(vec![a])),
                    _ => {
                        println!("type mismatch while reading column files {:?}", 
                            columns.get(col_i).map(|v| v.name.clone()));
                        mismatch = true;
                        col_result = None;
                        break
                    }
                };
            };

            if mismatch {
                return Err(ExecuteError::UnknownError("type mismatch while reading column files".into()));
            } else if let Some(v) = col_result {
                result.push(v);
            } else {
                result.push(SelectResult::Empty);
            }
        }

        Ok(result)
    }
}

pub async fn executor_loop(
    schema_manager: SchemaManager,
    mut plan_rx: Receiver<(QueryPlan, Arc<RwLock<QueryStateMarker>>)>
) {
    while let Some((plan, state_marker)) = plan_rx.recv().await {
        {
            *state_marker.write().await = QueryStateMarker::Running;
        }

        match plan {
            QueryPlan::Copy { source, column_order, contains_header, table } => {
            },
            QueryPlan::Select { columns, table } => {
                tokio::spawn(async move {
                    let locked_table = table.lock_select().await;
                    if let Some((table, data_dir)) = locked_table.table() {
                        match execute_select(&data_dir, table.columns()).await {
                            Ok(mut result) => {
                                *state_marker.write().await = QueryStateMarker::Completed(
                                    query_manager::QueryResult::Select(Arc::new(
                                        result.drain(..).map(|v| match v {
                                            SelectResult::Int64(v) => query_manager::Column::Int64(v),
                                            SelectResult::Str(v) => query_manager::Column::String(v),
                                            SelectResult::Empty => query_manager::Column::Empty,
                                        }).collect()
                                    ))
                                );
                            },
                            Err(err) => {
                                // none of the execute errors are supposed to happen and none can
                                // be handled by non-admin user
                                println!("execute error {:?}", err);
                                *state_marker.write().await = QueryStateMarker::Failed(
                                    QueryError::ExecuteError
                                );
                            }
                        }
                    } else {
                        *state_marker.write().await = QueryStateMarker::Failed(QueryError::TableDeleted)
                    }
                });
            },
        }
    }
}

pub async fn planner_loop(
    schema_manager: SchemaManager,
    mut query_rx: Receiver<(Uuid, Query, Arc<RwLock<QueryStateMarker>>)>, 
    mut plan_tx: Sender<(QueryPlan, Arc<RwLock<QueryStateMarker>>)>
) {
    while let Some((id, query, state_marker)) = query_rx.recv().await {
        {
            *state_marker.write().await = QueryStateMarker::Planning;
        }

        match plan_query(&schema_manager, query).await {
            Ok(plan) => {
                // send to query executor
                if let Err(err) = plan_tx.send((plan, state_marker.clone())).await {
                    println!("failed to send plan {:?}", err);
                    *state_marker.write().await = QueryStateMarker::Failed(QueryError::Unknown(err.to_string()));
                }
            },
            Err(err) => {
                *state_marker.write().await = QueryStateMarker::Failed(err);
            },
        }
    }
}

pub async fn start_query_executor(
    schema_manager: SchemaManager,
    query_manager: QueryManager, 
    mut query_rx: Receiver<(Uuid, Query, Arc<RwLock<QueryStateMarker>>)>
) -> JoinHandle<()> {
    let (plan_tx, plan_rx) = mpsc::channel::<(QueryPlan, Arc<RwLock<QueryStateMarker>>)>(16);

    // start query executor thread
    let executor_schema_manager = schema_manager.clone();
    let exeuctor_thread = spawn(move || {
        let executor_runtime = Builder::new_multi_thread()
            .enable_all()
            // .worker_threads(val)
            // reduce worker thread priority
            .build()
            .expect("failed to start executor runtime");

        executor_runtime.block_on(async move {
            executor_loop(executor_schema_manager, plan_rx).await
        });

        // for select: read all the files, zstd may need some computation threads but that shuld not be a problem
        // for copy: read csv, push to files
        // everything right now is i/o except for zstd decompression
    });

    // start query planner routine
    // query planning is not very cpu intensive right now
    tokio::spawn(planner_loop(schema_manager, query_rx, plan_tx))
}
