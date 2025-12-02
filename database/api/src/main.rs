use std::{env::args, process::{ExitCode}, sync::Arc, path::Path};
use async_trait::async_trait;
use axum_extra::extract::{CookieJar, Host};
use http::Method;
use openapi::{apis::{ErrorHandler, query::QueryApi, schema::SchemaApi}, models::{self, MultipleProblemsError, MultipleProblemsErrorProblemsInner, SystemInformation}, server};
use openapi::apis::metadata::*;
use openapi::apis::query::*;
use openapi::apis::schema::*;
use schema::{ColumnType, TableError};
use tokio::{sync::RwLock, time::Instant};

use crate::schema_manager::SchemaManager;

const AUTHOR: &'static str = "Franciszek Stachura";
const INTERFACE_VERSION: &'static str = "1.0.1";
const VERSION: &'static str = "0.0.1";

mod schema_manager;

#[derive(Clone)]
struct ApiImpl {
    start_time: Instant,
    schema_manager: Arc<RwLock<schema_manager::SchemaManager>>,
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

#[async_trait]
impl QueryApi<ApiError> for ApiImpl {
    async fn get_query_by_id(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        path_params: &models::GetQueryByIdPathParams,
    ) -> Result<GetQueryByIdResponse, ApiError> {
        Err(ApiError::Unimplemented)
    }

    async fn get_query_error(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        path_params: &models::GetQueryErrorPathParams,
    ) -> Result<GetQueryErrorResponse, ApiError> {
        Err(ApiError::Unimplemented)
    }

    async fn get_query_result(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        path_params: &models::GetQueryResultPathParams,
        body: &Option<models::GetQueryResultRequest>,
    ) -> Result<GetQueryResultResponse, ApiError> {
        Err(ApiError::Unimplemented)
    }

    async fn submit_query(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        body: &models::ExecuteQueryRequest,
    ) -> Result<SubmitQueryResponse, ApiError> {
        Err(ApiError::Unimplemented)
    }

    // optional in proj3
    async fn get_queries(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
    ) -> Result<GetQueriesResponse, ApiError> {
        Err(ApiError::Unimplemented)
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
                DeleteTableResponse::Status404(models::Error { message: "table deleted".into() })
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
            let api = Box::new(ApiImpl {
                start_time: Instant::now(),
                schema_manager: Arc::new(RwLock::new(SchemaManager::new("data".into()))),
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
