use std::collections;
use std::num::ParseIntError;
use std::string::ParseError;
use std::sync::{Arc};
use std::path::PathBuf;
use std::thread::spawn;
use std::iter::Enumerate;

use chrono::format::parse_and_remainder;
use tokio::io::{AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{MutexGuard, RwLock, RwLockReadGuard, mpsc};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::{fs::File};
use uuid::Uuid;
use itertools::Itertools;

use crate::query_manager::{self, Query, QueryError, QueryManager, QueryStateMarker};
use crate::schema_manager::{LockedTable, SchemaManager, TableLockedForCopy, TableManager};
use schema::{Column, ColumnType};
use crate::query_planner::{QueryPlan, plan_query};
use format::format::{CHUNK_HEADER_SIZE, ChunkHeader, DeserializerError, HEADER_SIZE, create_header, create_int64_chunk, create_str_chunk, parse_chunk_header, parse_header, parse_int64_chunk, parse_str_chunk};

const COPY_BATCH_SIZE: usize = 8192;

#[derive(Debug)]
#[allow(dead_code)]
enum ReadError {
    DeserializerError(String, DeserializerError),
    IoError(String, std::io::Error),
}

enum ReadResult {
    Int64(Vec<Vec<i64>>),
    Str(Vec<Vec<String>>),
}

async fn read_chunk_header(file: &mut File) -> Result<(u64, Vec<u8>), ReadError> {
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

async fn read_file(mut file: File) -> Result<ReadResult, ReadError> {
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
#[allow(dead_code)]
enum ExecuteError {
    IoError(String, std::io::Error),
    CsvError(csv::Error),
    WrongRecordSize(usize, usize),
    UnknownColumn(String),
    ReadError(ReadError),
    UnknownError(String),
    ParseIntError(ParseIntError),
    TableDeleted,
}

enum SelectResult {
    // file (assuming there aren't that many), chunk, data...
    Int64(Vec<Vec<Vec<i64>>>),
    Str(Vec<Vec<Vec<String>>>),
    Empty,
}


// in blocked context
async fn execute_copy(
    table_lock: TableLockedForCopy,
    source: PathBuf,
    has_header: bool,
    column_order: Vec<String>
) -> Result<(), ExecuteError> {
    // generate filenames
    // write files
    // drop read
    //      WHAT IF SOMEONE ACQUIRES WRITE HERE - no copy mutex = no write on directory. can write
    //      to schema but it's enforced in manager that only delete can happen in this case. if
    //      table gets deleted, we abort write finsh
    // write lock - it may now turn out that the table got deleted
    // push data to table, delete files is table got deleted

    let (table, data_dir) = if let Some(v) = table_lock.table() {
        v
    } else {
        return Err(ExecuteError::TableDeleted)
    };

    let targets = table_lock.copy_targets().clone();

    let final_column_order = {
        let mut final_column_order = vec![];
        for o in column_order {
            let mut found = false;
            for (name, typ, filename) in targets.clone() {
                if *name == o {
                    let path = data_dir.join(filename);
                    final_column_order.push((name.clone(), typ, path));
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(ExecuteError::UnknownColumn(o));
            }
        }

        final_column_order
    };

    println!("final column order {:?}", final_column_order);

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(has_header)
        .from_path(source)
        .map_err(ExecuteError::CsvError)?;

    let mut writing_routines = JoinSet::new();

    let mut column_channels = {
        let mut column_channels = vec![];

        for (name, typ, path) in final_column_order {
            println!("opened {path:?}");

            let mut file = File::options()
                .write(true)
                .truncate(true)
                .create(true)
                .open(path.clone())
                .await
                .map_err(|err| ExecuteError::IoError(
                    format!("failed to open column path {:?} for writing", path),
                    err
                ))?; // may quit, send handles will be dropped, so routines will exit

            let mut header_buf = vec![];
            let format_type = match typ {
                ColumnType::INT64 => format::format::ColumnType::Int64,
                ColumnType::VARCHAR => format::format::ColumnType::Str,
            };
            create_header(
                &mut header_buf,
                format_type,
                0
            ).map_err(|e| ExecuteError::IoError("failed to create header".to_string(), e))?;
            file.write_all(&header_buf).await
                .map_err(|e| ExecuteError::IoError("failed to write header".to_string(), e))?;

            let (tx, mut rx) = mpsc::channel::<Vec<String>>(16);

            let handle = writing_routines.spawn(async move {
                let mut result = Ok(());
                let mut chunks = 0;

                while let Some(chunk) = rx.recv().await {
                    chunks += 1;
                    result = match typ {
                        ColumnType::INT64 => {
                            let nums: Result<Vec<i64>, _> = chunk.iter().map(|v| v.parse::<i64>()).collect();
                            if let Ok(nums) = nums {
                                let bytes = create_int64_chunk(&nums);
                                file.write(&bytes).await
                                    .map_err(|e| ExecuteError::IoError("failed to write ints".to_string(), e))
                                    .map(|_| ())
                            } else {
                                nums
                                    .map_err(|e| ExecuteError::ParseIntError(e))
                                    .map(|_| ())
                            }
                        },
                        ColumnType::VARCHAR => {
                            let bytes = create_str_chunk(&chunk);
                            if let Ok(bytes) = bytes {
                                file.write(&bytes).await
                                    .map_err(|e| ExecuteError::IoError("failed to write strs".to_string(), e))
                                    .map(|_| ())
                            } else {
                                bytes
                                    .map_err(|e| ExecuteError::IoError("failed to create str chunk".to_string(), e))
                                    .map(|_| ())
                            }
                        },
                    };

                    if result.is_err() {
                        println!("failed to write chunk {:?}", result);
                        break; // quits, next queue send will fail because rx was dropped
                    }
                }

                if result.is_ok() {
                    file.seek(std::io::SeekFrom::Start(0)).await
                        .map_err(|e| ExecuteError::IoError("failed to seek".to_string(), e))?;
                    let mut header_buf = vec![];
                    create_header(
                        &mut header_buf,
                        format_type,
                        chunks
                    ).map_err(|e| ExecuteError::IoError("failed to create header".to_string(), e))?;
                    file.write_all(&header_buf).await
                        .map_err(|e| ExecuteError::IoError("failed to write header".to_string(), e))?;
                    Ok(())
                } else {
                    println!("error {result:?}");
                    result
                }
            });

            column_channels.push((tx, handle));
        }

        column_channels
    };

    println!("spawned routines");

    let mut result = Ok(());
    let mut chunks = vec![Vec::with_capacity(COPY_BATCH_SIZE); column_channels.len()];
    let mut chunk_len = 0;

    for record in reader.records() {
        match record {
            Ok(v) => {
                if v.len() < column_channels.len() {
                    result = Err(ExecuteError::WrongRecordSize(v.len(), column_channels.len()));
                    break
                }

                for (record, chunk) in v.iter().zip(chunks.iter_mut()) {
                    chunk.push(record.to_string());
                }

                if chunk_len >= CHUNK_HEADER_SIZE {
                    for (chunk, (tx, handle)) in chunks.drain(..).zip(column_channels.iter()) {
                        if let Err(err) = tx.send(chunk).await {
                            println!("send error {:?}", err);
                            result = Err(ExecuteError::UnknownError(format!("{:?}", err)));
                            break;
                        }
                    }

                    chunk_len = 0;
                    chunks = vec![Vec::with_capacity(COPY_BATCH_SIZE); column_channels.len()];
                } else {
                    chunk_len += 1;
                }
            },
            Err(err) => {
                result = Err(ExecuteError::CsvError(err));
                break
            },
        }
    }


    if result.is_err() {
        writing_routines.abort_all();
    } else if chunk_len >= 0 {
        for (chunk, (tx, handle)) in chunks.drain(..).zip(column_channels.iter()) {
            if let Err(err) = tx.send(chunk).await {
                println!("send error {:?}", err);
                result = Err(ExecuteError::UnknownError(format!("{:?}", err)));
                break;
            }
        }
    }

    for (tx, handle) in column_channels.drain(..) {
        drop(tx)
    }

    while let Some(join_result) = writing_routines.join_next().await {
        if let Err(err) = join_result {
            if err.is_panic() {
                println!("coroutine panicked {err:?}");
                result = Err(ExecuteError::UnknownError(format!("writing coroutine panicked {err:?}")));
            }
        }
    }

    if result.is_ok() {
        if let Err(err) = table_lock.finish_write().await {
            println!("failed to finish write {err:?}");
            Err(ExecuteError::UnknownError(format!("{err:?}")))
        } else {
            Ok(())
        }
    } else {
        result
    }
}

async fn execute_select(data_dir: &PathBuf, columns: &Vec<Column>) -> Result<Vec<SelectResult>, ExecuteError> {
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
                println!("copy query");
                let state_marker_clone = state_marker.clone();
                let spawn_result = tokio::spawn(async move {
                    println!("in copy spawn");
                    let locked_table = table.lock_copy().await;
                    println!("locked table");
                    match execute_copy(locked_table, source, contains_header, column_order).await {
                        Ok(()) => {
                            *state_marker.write().await = QueryStateMarker::Completed(
                                query_manager::QueryResult::Copy
                            );
                        },
                        Err(err) => {
                            println!("copy error {:?}", err);
                            *state_marker.write().await = QueryStateMarker::Failed(
                                QueryError::ExecuteError
                            );
                        },
                    }
                });
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
                println!("failed to plan query {:?}", err);
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
