use async_trait::async_trait;
use axum_extra::extract::{CookieJar, Host};
use http::Method;
use schema::{ColumnType, TableError};
use std::{env::args, path::Path, process::ExitCode, str::FromStr, sync::Arc};
use tokio::{sync::RwLock, time::Instant};
use uuid::Uuid;

use crate::apis::{ErrorHandler, metadata::*, query::*, schema::*};
use crate::consts::{API_VERSION, AUTHOR, VERSION};
use crate::models::{
    self, MultipleProblemsError, MultipleProblemsErrorProblemsInner, ShallowQuery,
    SystemInformation,
};
use crate::query_manager;
use crate::query_manager::{QueryManager, QueryStateMarker};
use crate::schema_manager::SchemaError;
use crate::{query_manager::QueryError, schema_manager::SchemaManager};

#[derive(Clone)]
pub struct ApiImpl {
    pub start_time: Instant,
    pub schema_manager: SchemaManager,
    pub query_manager: QueryManager,
}

#[derive(Debug)]
pub enum ApiError {
    Unimplemented,
    UnknownError(String),
}

impl ErrorHandler<ApiError> for ApiImpl {}

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
            interface_version: Some(API_VERSION.to_string()),
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
        query_manager::Query::Copy {
            source,
            target,
            columns,
            contains_header,
        } => models::QueryQueryDefinition::CopyQuery(models::CopyQuery {
            source_filepath: source.to_string_lossy().to_string(),
            destination_table_name: target.clone(),
            destination_columns: columns.clone(),
            does_csv_contain_header: Some(*contains_header),
        }),
        query_manager::Query::Select { table } => {
            models::QueryQueryDefinition::SelectQuery(models::SelectQuery {
                table_name: table.clone(),
            })
        }
    }
}

fn query_error_to_problems(error: &QueryError) -> models::MultipleProblemsError {
    models::MultipleProblemsError { problems: vec![] }
}

fn query_result_to_response(
    result: &query_manager::QueryResult,
    row_limit: Option<usize>,
) -> Vec<models::QueryResultInner> {
    let result = match result {
        query_manager::QueryResult::Copy => models::QueryResultInner {
            row_count: Some(0),
            columns: Some(vec![]),
        },
        query_manager::QueryResult::Select(columns) => {
            let mut row_count = None;

            if let Some(row_limit) = row_limit {
                let columns = columns
                    .iter()
                    .map(|col| match col {
                        query_manager::Column::String(c) => {
                            // TODO custom type with custom serialize to avoid copying here?
                            let column: Vec<String> = c
                                .iter()
                                .flatten()
                                .flatten()
                                .take(row_limit)
                                .cloned()
                                .collect();
                            assert!(row_count.is_none() || row_count.unwrap() == column.len());
                            row_count = Some(column.len());
                            models::QueryResultInnerColumnsInner::VecOfString(Arc::new(column))
                        }
                        query_manager::Column::Int64(c) => {
                            let column: Vec<i64> = c
                                .iter()
                                .flatten()
                                .flatten()
                                .take(row_limit)
                                .cloned()
                                .collect();
                            assert!(row_count.is_none() || row_count.unwrap() == column.len());
                            row_count = Some(column.len());
                            models::QueryResultInnerColumnsInner::VecOfi64(Arc::new(column))
                        }
                    })
                    .collect();

                models::QueryResultInner {
                    row_count: Some(row_count.unwrap_or(0)),
                    columns: Some(columns),
                }
            } else {
                let columns = columns
                    .iter()
                    .map(|col| match col {
                        query_manager::Column::String(c) => {
                            let column: Vec<String> =
                                c.iter().flatten().flatten().cloned().collect();
                            assert!(row_count.is_none() || row_count.unwrap() == column.len());
                            row_count = Some(column.len());
                            models::QueryResultInnerColumnsInner::VecOfString(Arc::new(column))
                        }
                        query_manager::Column::Int64(c) => {
                            let column: Vec<i64> = c.iter().flatten().flatten().cloned().collect();
                            assert!(row_count.is_none() || row_count.unwrap() == column.len());
                            row_count = Some(column.len());
                            models::QueryResultInnerColumnsInner::VecOfi64(Arc::new(
                                c.iter().flatten().flatten().cloned().collect(),
                            ))
                        }
                    })
                    .collect();

                models::QueryResultInner {
                    row_count: Some(row_count.unwrap_or(0)),
                    columns: Some(columns),
                }
            }
        }
    };

    // TODO why array?
    vec![result]
}

fn model_query_to_manager_query(
    query: models::QueryQueryDefinition,
) -> Result<query_manager::Query, models::MultipleProblemsError> {
    Ok(match query {
        models::QueryQueryDefinition::SelectQuery(s) => query_manager::Query::Select {
            table: s.table_name,
        },
        models::QueryQueryDefinition::CopyQuery(s) => query_manager::Query::Copy {
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
            }));
        };

        match self.query_manager.get_query_state(&qs).await {
            Some(query) => {
                let query_state = query.1.read().await;
                Ok(GetQueryByIdResponse::Status200(models::Query {
                    query_id: path_params.query_id.clone(),
                    status: query_state_marker_to_status(&query_state),
                    is_result_available: Some(match *query_state {
                        QueryStateMarker::Completed(_) => true,
                        QueryStateMarker::Failed(_) => true,
                        _ => false,
                    }),
                    query_definition: Some(manager_query_to_model(&query.0)),
                }))
            }
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
            }));
        };

        Ok(
            if let Some(query) = self.query_manager.get_query_state(&qs).await {
                let query_state = query.1.read().await;
                match &*query_state {
                    QueryStateMarker::Failed(error) => {
                        GetQueryErrorResponse::Status200(query_error_to_problems(&error))
                    }
                    _ => GetQueryErrorResponse::Status400(models::Error {
                        message: "query did not fail".to_string(),
                    }),
                }
            } else {
                GetQueryErrorResponse::Status404(models::Error {
                    message: "no query with this id".to_string(),
                })
            },
        )
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
            }));
        };

        let query = if let Some(true) = flush_result {
            self.query_manager.consume_query(&id).await
        } else {
            self.query_manager.get_query_state(&id).await
        };

        Ok(match query {
            Some(query) => {
                let query_state = query.1.read().await;
                match &*query_state {
                    QueryStateMarker::Completed(result) => GetQueryResultResponse::Status200(
                        query_result_to_response(result, row_limit),
                    ),
                    _ => GetQueryResultResponse::Status400(models::Error {
                        message: "query is not completed".to_string(),
                    }),
                }
            }
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
            Err(err) => return Ok(SubmitQueryResponse::Status400(err)),
        };

        let state = self
            .query_manager
            .submit_query(query)
            .await
            .map_err(into_unknown)?;

        Ok(SubmitQueryResponse::Status200(state.0.to_string()))
    }

    async fn get_queries(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<GetQueriesResponse, ApiError> {
        let query_list = self
            .query_manager
            .list_queries()
            .await
            .map_err(into_unknown)?;

        Ok(GetQueriesResponse::Status200(
            query_list
                .iter()
                .map(|(uuid, marker)| ShallowQuery {
                    query_id: uuid.to_string(),
                    status: query_state_marker_to_status(marker),
                })
                .collect(),
        ))
    }
}

fn schema_error_to_model_problems(err: SchemaError) -> Option<MultipleProblemsError> {
    Some(match err {
        SchemaError::TableExists(name) => MultipleProblemsError {
            problems: vec![MultipleProblemsErrorProblemsInner {
                error: "table exists".into(),
                context: Some(name),
            }],
        },
        SchemaError::UnknownTable(name) => MultipleProblemsError {
            problems: vec![MultipleProblemsErrorProblemsInner {
                error: "unknown table".into(),
                context: Some(name),
            }],
        },
        SchemaError::TableError(TableError::InvalidTableName(name)) => MultipleProblemsError {
            problems: vec![MultipleProblemsErrorProblemsInner {
                error: "invalid table name".into(),
                context: Some(name),
            }],
        },
        SchemaError::TableError(TableError::InvalidColumnName(table, names)) => {
            MultipleProblemsError {
                problems: vec![MultipleProblemsErrorProblemsInner {
                    error: "invalid column name(s)".to_string() + &names.join(", "),
                    context: Some(table),
                }],
            }
        }
        SchemaError::TableError(TableError::NoColumns(name)) => MultipleProblemsError {
            problems: vec![MultipleProblemsErrorProblemsInner {
                error: "cannot create table without columns".into(),
                context: Some(name),
            }],
        },
        SchemaError::TableError(TableError::DuplicatedColumns(name, mut columns)) => {
            MultipleProblemsError {
                problems: columns
                    .drain(..)
                    .map(|c| MultipleProblemsErrorProblemsInner {
                        error: format!("duplicated column: {c}"),
                        context: Some(name.clone()),
                    })
                    .collect(),
            }
        }
        _ => {
            println!("encountered unknown error {err:?}");
            return None;
        }
    })
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
        let columns: Vec<(String, ColumnType)> = body
            .columns
            .iter()
            .map(|c| {
                (
                    c.name.clone(),
                    match c.r_type {
                        models::LogicalColumnType::Int64 => schema::ColumnType::INT64,
                        models::LogicalColumnType::Varchar => schema::ColumnType::VARCHAR,
                    },
                )
            })
            .collect();

        Ok(
            match self
                .schema_manager
                .create_table(body.name.clone(), &columns)
                .await
            {
                Ok(_) => CreateTableResponse::Status200(body.name.clone()),
                Err(err) => CreateTableResponse::Status400(
                    schema_error_to_model_problems(err)
                        .ok_or(ApiError::UnknownError("unknown error".to_string()))?,
                ),
            },
        )
    }

    async fn delete_table(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::DeleteTablePathParams,
    ) -> Result<DeleteTableResponse, ApiError> {
        Ok(
            match self
                .schema_manager
                .delete_table(&path_params.table_id)
                .await
            {
                Ok(_) => DeleteTableResponse::Status200,
                Err(err) => DeleteTableResponse::Status404(models::Error {
                    message: "table does not exit".into(),
                }),
            },
        )
    }

    async fn get_table_by_id(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        path_params: &models::GetTableByIdPathParams,
    ) -> Result<GetTableByIdResponse, ApiError> {
        Ok(
            match self
                .schema_manager
                .get_table_manager(&path_params.table_id)
                .await
            {
                Some(table_manager) => {
                    let table_guard = table_manager.read_schema().await;
                    let table = if let Some(ref table) = table_guard {
                        table
                    } else {
                        return Ok(GetTableByIdResponse::Status404(models::Error {
                            message: "unknown table".to_string(),
                        }));
                    };

                    let columns = table
                        .table
                        .columns()
                        .iter()
                        .map(|c| models::Column {
                            name: c.name.clone(),
                            r_type: match c.column_type {
                                schema::ColumnType::INT64 => models::LogicalColumnType::Int64,
                                schema::ColumnType::VARCHAR => models::LogicalColumnType::Varchar,
                            },
                        })
                        .collect();

                    GetTableByIdResponse::Status200(models::TableSchema {
                        name: path_params.table_id.clone(),
                        columns,
                    })
                }
                None => GetTableByIdResponse::Status404(models::Error {
                    message: format!("unknown table: {}", path_params.table_id),
                }),
            },
        )
    }

    async fn get_tables(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
    ) -> Result<GetTablesResponse, ApiError> {
        let tables = self
            .schema_manager
            .list_tables()
            .await
            .iter()
            .map(|t| models::ShallowTable {
                table_id: Some(t.clone()),
                name: t.clone(),
            })
            .collect();

        Ok(GetTablesResponse::Status200(tables))
    }
}
