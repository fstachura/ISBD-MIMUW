use std::{env::args, path::Path, process::ExitCode, str::FromStr, sync::Arc};
use async_trait::async_trait;
use axum_extra::extract::{CookieJar, Host};
use http::Method;
use openapi::{apis::{ErrorHandler, query::QueryApi, schema::SchemaApi}, models::{self, MultipleProblemsError, MultipleProblemsErrorProblemsInner, ShallowQuery, SystemInformation}, server};
use openapi::apis::metadata::*;
use openapi::apis::query::*;
use openapi::apis::schema::*;
use schema::{ColumnType, TableError};
use tokio::{sync::RwLock, time::Instant};
use uuid::Uuid;

mod schema_manager;
use crate::{query_manager::QueryError, schema_manager::SchemaManager};

mod query_manager;
use crate::query_manager::{QueryManager, QueryStateMarker};

const AUTHOR: &'static str = "Franciszek Stachura";
const INTERFACE_VERSION: &'static str = "1.0.1";
const VERSION: &'static str = "0.0.1";


#[derive(Clone)]
struct ApiImpl {
    start_time: Instant,
    schema_manager: Arc<RwLock<SchemaManager>>,
    query_manager: QueryManager,
}

#[derive(Debug)]
enum ApiError {
    Unimplemented,
    UnknownError(String),
}

impl ErrorHandler<ApiError> for ApiImpl {
}

fn into_unknown(err: impl std::error::Error) -> ApiError {
    println!("got error {err}");
    ApiError::UnknownError("unknown".into())
}

#[async_trait]
impl MetadataApi<ApiError> for ApiImpl {
    async fn get_system_info(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<GetSystemInfoResponse, ApiError> {
        Ok(GetSystemInfoResponse::Status200(SystemInformation {
            interface_version: Some(INTERFACE_VERSION.to_string()),
            version: VERSION.to_string(),
            author: Some(AUTHOR.to_string()),
            uptime: Instant::now().duration_since(self.start_time).as_secs(),
        }))
    }
}

fn query_state_marker_to_status(marker: &QueryStateMarker) -> models::QueryStatus {
    match marker {
        QueryStateMarker::Created => models::QueryStatus::Created,
        QueryStateMarker::Planning => models::QueryStatus::Planning,
        QueryStateMarker::Running => models::QueryStatus::Running,
        QueryStateMarker::Completed(_) => models::QueryStatus::Completed,
        QueryStateMarker::Failed(_) => models::QueryStatus::Failed,
    }
}

fn manager_query_to_model(query: &query_manager::Query) -> models::QueryQueryDefinition {
    match query {
        query_manager::Query::Copy { source, target, columns, contains_header } =>
            models::QueryQueryDefinition::CopyQuery(models::CopyQuery {
                source_filepath: source.to_string_lossy().to_string(),
                destination_table_name: target.clone(),
                destination_columns: columns.clone(),
                does_csv_contain_header: Some(*contains_header),
            }),
        query_manager::Query::Select { table } =>
            models::QueryQueryDefinition::SelectQuery(models::SelectQuery {
                table_name: Some(table.clone()),
            }),
    }
}

fn query_error_to_problems(error: &QueryError) -> models::MultipleProblemsError {
    // TODO
    models::MultipleProblemsError {
        problems: vec![],
    }
}

fn query_result_to_response(result: &query_manager::QueryResult, row_limit: Option<usize>) -> Vec<models::QueryResultInner> {
    let result = match result {
        query_manager::QueryResult::Copy =>
            models::QueryResultInner {
                row_count: Some(0),
                columns: Some(vec![]),
            },
        query_manager::QueryResult::Select(columns) => {
            let mut row_count = None;

            if let Some(row_limit) = row_limit {
                let columns = columns.iter().map(|col| match col {
                    query_manager::Column::String(c) => {
                        // TODO custom type with custom serialize to avoid copying here?
                        let column = c.iter().take(row_limit).cloned().collect();
                        assert!(row_count.is_none() || row_count.unwrap() == c.len());
                        row_count = Some(c.len());
                        models::QueryResultInnerColumnsInner::VecOfString(Arc::new(column))
                    },
                    query_manager::Column::Int64(c) => {
                        let column = c.iter().take(row_limit).map(|v| *v).collect();
                        assert!(row_count.is_none() || row_count.unwrap() == c.len());
                        row_count = Some(c.len());
                        models::QueryResultInnerColumnsInner::VecOfi64(Arc::new(column))
                    }
                }).collect();

                models::QueryResultInner {
                    row_count: Some(row_count.unwrap_or(0)),
                    columns: Some(columns),
                }
            } else {
                let columns = columns.iter().map(|col| match col {
                    query_manager::Column::String(c) => {
                        assert!(row_count.is_none() || row_count.unwrap() == c.len());
                        row_count = Some(c.len());
                        models::QueryResultInnerColumnsInner::VecOfString(c.clone())
                    },
                    query_manager::Column::Int64(c) => {
                        assert!(row_count.is_none() || row_count.unwrap() == c.len());
                        row_count = Some(c.len());
                        models::QueryResultInnerColumnsInner::VecOfi64(c.clone())
                    }
                }).collect();

                models::QueryResultInner {
                    row_count: Some(row_count.unwrap_or(0)),
                    columns: Some(columns),
                }
            }
        },
    };

    // TODO what's the point of this again?
    vec![result]
}

fn model_query_to_manager_query(query: models::QueryQueryDefinition)
    -> Result<query_manager::Query, models::MultipleProblemsError> {

    Ok(match query {
        models::QueryQueryDefinition::SelectQuery(s) =>
            query_manager::Query::Select { 
                table: s.table_name.ok_or(models::MultipleProblemsError {
                    problems: vec![
                        models::MultipleProblemsErrorProblemsInner {
                            error: "table name not provided".to_string(),
                            context: None,
                        }
                    ],
                })?
            },
        models::QueryQueryDefinition::CopyQuery(s) =>
            query_manager::Query::Copy {
                source: s.source_filepath.into(),
                target: s.destination_table_name,
                columns: s.destination_columns,
                contains_header: s.does_csv_contain_header.unwrap_or(false),
            },
    })
}

#[async_trait]
impl QueryApi<ApiError> for ApiImpl {
    async fn get_query_by_id(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetQueryByIdPathParams,
    ) -> Result<GetQueryByIdResponse, ApiError> {
        let qs = if let Ok(id) = Uuid::from_str(&path_params.query_id) {
            id
        } else {
            return Ok(GetQueryByIdResponse::Status404(models::Error { 
                message: "not an uuid".to_string(),
            }))
        };

        match self.query_manager.get_query_state(&qs).await {
            Some(query) => {
                let query = query.read().await;
                Ok(GetQueryByIdResponse::Status200(models::Query {
                    query_id: path_params.query_id.clone(),
                    status: query_state_marker_to_status(&query.1),
                    is_result_available: Some(match query.1 {
                        QueryStateMarker::Completed(_) => true,
                        QueryStateMarker::Failed(_) => true,
                        _ => false,
                    }),
                    query_definition: Some(manager_query_to_model(&query.0)),
                }))
            },
            None => Ok(GetQueryByIdResponse::Status404(models::Error { 
                message: "no query with this id".to_string(),
            })),
        }
    }

    async fn get_query_error(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetQueryErrorPathParams,
    ) -> Result<GetQueryErrorResponse, ApiError> {
        let qs = if let Ok(id) = Uuid::from_str(&path_params.query_id) {
            id
        } else {
            return Ok(GetQueryErrorResponse::Status404(models::Error { 
                message: "not an uuid".to_string(),
            }))
        };

        Ok(if let Some(query) = self.query_manager.get_query_state(&qs).await {
            let query = query.read().await;
            match &query.1 {
                QueryStateMarker::Failed(error) =>
                    GetQueryErrorResponse::Status200(query_error_to_problems(&error)),
                _ =>
                    GetQueryErrorResponse::Status400(models::Error {
                        message: "query is not failed".to_string(),
                    }),
            }
        } else {
            GetQueryErrorResponse::Status404(models::Error { 
            message: "no query with this id".to_string(),
            })
        })
    }

    async fn get_query_result(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetQueryResultPathParams,
        body: &Option<models::GetQueryResultRequest>,
    ) -> Result<GetQueryResultResponse, ApiError> {
        let row_limit = body.clone().map(|v| v.row_limit).flatten();
        let flush_result = body.clone().map(|v| v.flush_result).flatten();

        let id = if let Ok(id) = Uuid::from_str(&path_params.query_id) {
            id
        } else {
            return Ok(GetQueryResultResponse::Status400(models::Error { 
                message: "not an uuid".to_string(),
            }))
        };

        let query = if let Some(true) = flush_result {
            self.query_manager.consume_query(&id).await
        } else {
            self.query_manager.get_query_state(&id).await
        };

        Ok(match query {
            Some(query) => {
                let query = query.read().await;
                match &query.1 {
                    QueryStateMarker::Completed(result) =>
                        // TODO why array of arrays?
                        GetQueryResultResponse::Status200(query_result_to_response(result, row_limit)),
                    _ =>
                        GetQueryResultResponse::Status400(models::Error {
                            message: "query is not completed".to_string(),
                        }),
                }
            },
            None => GetQueryResultResponse::Status404(models::Error { 
                message: "no query with this id".to_string(),
            }),
        })
    }

    async fn submit_query(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::ExecuteQueryRequest,
    ) -> Result<SubmitQueryResponse, ApiError> {
        let query = match model_query_to_manager_query(body.query_definition.clone()) {
            Ok(query) => query,
            Err(err) => {
                return Ok(SubmitQueryResponse::Status400(err))
            }
        };

        let id = self.query_manager.submit_query(query)
            .await.map_err(into_unknown)?;

        Ok(SubmitQueryResponse::Status200(id.to_string()))
    }

    async fn get_queries(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<GetQueriesResponse, ApiError> {
        let query_list = self.query_manager.list_queries()
            .await.map_err(into_unknown)?;

        Ok(GetQueriesResponse::Status200(
            query_list.iter().map(|(uuid, marker)| ShallowQuery {
                query_id: uuid.to_string(),
                status: query_state_marker_to_status(marker),
            }).collect()
        ))
    }
}

fn table_error_to_model_problems(err: TableError) -> MultipleProblemsError {
    match err {
        TableError::TableExists(name) =>
            MultipleProblemsError { problems: vec![ MultipleProblemsErrorProblemsInner {
                error: "table exists".into(), context: Some(name),
            } ] },
        TableError::UnknownTable(name) =>
            MultipleProblemsError { problems: vec![ MultipleProblemsErrorProblemsInner {
                error: "unknown table".into(), context: Some(name),
            } ] },
        TableError::InvalidTableName(name) =>
            MultipleProblemsError { problems: vec![ MultipleProblemsErrorProblemsInner {
                error: "invalid table name".into(), context: Some(name),
            } ] },
        TableError::InvalidColumnName(names) =>
            MultipleProblemsError { problems: vec![ MultipleProblemsErrorProblemsInner {
                error: "invalid column name(s)".into(), context: Some(names.join(", ")),
            } ] },
        TableError::NoColumns(name) =>
            MultipleProblemsError { problems: vec![ MultipleProblemsErrorProblemsInner {
                error: "cannot create table without columns".into(), context: Some(name),
            } ] },
    }
}

#[async_trait]
impl SchemaApi<ApiError> for ApiImpl {
    async fn create_table(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        body: &models::TableSchema,
    ) -> Result<CreateTableResponse, ApiError> {
        let sm = self.schema_manager.write().await;
        let mut s = sm.get_mut_schema().await.map_err(into_unknown)?;
        let columns: Vec<(String, ColumnType)> = body.columns.iter().map(|c| (
            c.name.clone(),
            match c.r_type {
                models::LogicalColumnType::Int64 => schema::ColumnType::INT64,
                models::LogicalColumnType::Varchar => schema::ColumnType::VARCHAR,
            },
        )).collect();

        Ok(match s.get_mut().create_table(body.name.clone(), &columns) {
            Ok(_) => {
                s.flush().map_err(into_unknown)?;
                CreateTableResponse::Status200(body.name.clone())
            }
            Err(err) =>
                CreateTableResponse::Status400(table_error_to_model_problems(err)),
        })
    }

    async fn delete_table(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::DeleteTablePathParams,
    ) -> Result<DeleteTableResponse, ApiError> {
        let sm = self.schema_manager.write().await;
        let mut s = sm.get_mut_schema().await.map_err(into_unknown)?;

        Ok(match s.get_mut().delete_table(path_params.table_id.clone()) {
            Ok(_) => {
                s.flush().map_err(into_unknown)?;
                // TODO delete table files
                DeleteTableResponse::Status200
            }
            Err(err) =>
                DeleteTableResponse::Status404(models::Error { message: "table does not exit".into() })
        })
    }

    async fn get_table_by_id(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetTableByIdPathParams,
    ) -> Result<GetTableByIdResponse, ApiError> {
        let sm = self.schema_manager.read().await;
        let s = sm.get_schema().await.map_err(into_unknown)?;

        let tables = s.get().get_tables();
        Ok(match tables.get(&path_params.table_id) {
            Some(table) => {
                GetTableByIdResponse::Status200(models::TableSchema {
                    name: path_params.table_id.clone(),
                    columns: table.columns.iter().map(|c| models::Column {
                        name: c.name.clone(),
                        r_type: match c.column_type {
                            schema::ColumnType::INT64 => models::LogicalColumnType::Int64,
                            schema::ColumnType::VARCHAR => models::LogicalColumnType::Varchar,
                        },
                    }).collect(),
                })
            },
            None =>
                GetTableByIdResponse::Status404(models::Error {
                    message: format!("unknown table: {}", path_params.table_id),
                })
        })
    }

    async fn get_tables(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<GetTablesResponse, ApiError> {
        let sm = self.schema_manager.read().await;
        let s = sm.get_schema().await.map_err(into_unknown)?;

        let tables = s.get().get_tables();
        let tables = tables.iter().map(|t| models::ShallowTable {
            table_id: Some(t.0.clone()),
            name: t.0.clone(),
        }).collect();
        Ok(GetTablesResponse::Status200(tables))
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = args();
    if args.len() != 3 {
        println!("usage: (init|serve) data_dir");
        return ExitCode::FAILURE;
    }

    let _ = args.next().unwrap();
    let cmd = args.next().unwrap();
    let data_dir = args.next().unwrap();

    match cmd.as_str() {
        "init" => {
            schema::create_schema_dir(Path::new(&data_dir)).unwrap();
            println!("data directory initialized");
            return ExitCode::SUCCESS;
        },
        "serve" => {
            let (qm, query_receiver) = QueryManager::new();
            let api = Box::new(ApiImpl {
                start_time: Instant::now(),
                schema_manager: Arc::new(RwLock::new(SchemaManager::new("data".into()))),
                query_manager: qm,
            });
            let app = server::new(api);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
            axum::serve(listener, app).await.unwrap();
        },
        _ => {
            println!("invalid cmd. valid cmd: init, serve");
            return ExitCode::FAILURE;
        },
    };

    ExitCode::SUCCESS
}
