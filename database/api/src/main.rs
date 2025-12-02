use async_trait::async_trait;
use axum_extra::extract::{CookieJar, Host};
use http::Method;
use openapi::{apis::{ErrorHandler, query::QueryApi, schema::SchemaApi}, models, server};
use openapi::apis::metadata::*;
use openapi::apis::query::*;
use openapi::apis::schema::*;

#[derive(Clone)]
struct ApiImpl {
}

#[derive(Debug)]
enum ApiError {
    Unimplemented,
    UnknownError(String),
}

impl ErrorHandler<ApiError> for ApiImpl {
}

#[async_trait]
impl MetadataApi<ApiError> for ApiImpl {
    async fn get_system_info(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
    ) -> Result<GetSystemInfoResponse, ApiError> {
        Err(ApiError::Unimplemented)
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

    async fn get_queries(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
    ) -> Result<GetQueriesResponse, ApiError> {
        Err(ApiError::Unimplemented)
    }
}

#[async_trait]
impl SchemaApi<ApiError> for ApiImpl {
    async fn create_table(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        body: &models::TableSchema,
    ) -> Result<CreateTableResponse, ApiError> {
        Err(ApiError::Unimplemented)
    }

    async fn delete_table(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        path_params: &models::DeleteTablePathParams,
    ) -> Result<DeleteTableResponse, ApiError> {
        Err(ApiError::Unimplemented)
    }

    async fn get_table_by_id(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        path_params: &models::GetTableByIdPathParams,
    ) -> Result<GetTableByIdResponse, ApiError> {
        Err(ApiError::Unimplemented)
    }

    async fn get_tables(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
    ) -> Result<GetTablesResponse, ApiError> {
        Err(ApiError::Unimplemented)
    }
}

#[tokio::main]
async fn main() {
    let api = Box::new(ApiImpl {});
    let app = server::new(api);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
