use std::collections::HashMap;

use axum::{body::Body, extract::*, response::Response, routing::*};
use axum_extra::extract::{CookieJar, Host, Query as QueryExtra};
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header::CONTENT_TYPE};
use tracing::error;
use tower_http::trace::TraceLayer;
use validator::{Validate, ValidationErrors};

#[allow(unused_imports)]
use crate::{apis, models};

/// Setup API Server.
pub fn new<I, A, E>(api_impl: I) -> Router
where
    I: AsRef<A> + Clone + Send + Sync + 'static,
    A: apis::query::QueryApi<E>
        + apis::query::QueryApi<E>
        + apis::metadata::MetadataApi<E>
        + apis::schema::SchemaApi<E>
        + Send
        + Sync
        + 'static,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    Router::new()
        .route("/error/{query_id}", get(get_query_error::<I, A, E>))
        .route("/queries", get(get_queries::<I, A, E>))
        .route("/query", post(submit_query::<I, A, E>))
        .route("/query/{query_id}", get(get_query_by_id::<I, A, E>))
        .route("/result/{query_id}", get(get_query_result::<I, A, E>))
        .route("/system/info", get(get_system_info::<I, A, E>))
        .route("/table", put(create_table::<I, A, E>))
        .route(
            "/table/{table_id}",
            delete(delete_table::<I, A, E>).get(get_table_by_id::<I, A, E>),
        )
        .route("/tables", get(get_tables::<I, A, E>))
        .with_state(api_impl)
        .layer(TraceLayer::new_for_http())
}

#[tracing::instrument(skip_all)]
fn get_query_by_id_validation(
    path_params: models::GetQueryByIdPathParams,
) -> std::result::Result<(models::GetQueryByIdPathParams,), ValidationErrors> {
    path_params.validate()?;

    Ok((path_params,))
}

async fn generic_json_response<T: Send + Sync + 'static + serde::Serialize>(status: u16, body: T) -> 
    Result<Result<http::response::Response<Body>, http::Error>, StatusCode> {

    let mut response = Response::builder();
    let mut response = response.status(status);
    {
        let mut response_headers = response.headers_mut().unwrap();
        response_headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }

    let body_content = tokio::task::spawn_blocking(move || {
        serde_json::to_vec(&body).map_err(|e| {
            error!(error = ?e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
    })
    .await
    .unwrap()?;
    Ok(response.body(Body::from(body_content)))
}

macro_rules! or_bad_request {
    ($validation:ident) => {
        {
            or_bad_request!($validation, path_params)
        }
    };
    ($validation:ident, $target:tt) => {
        {
            let Ok($target) = $validation else {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from($validation.unwrap_err().to_string()))
                    .map_err(|_| StatusCode::BAD_REQUEST)
            };
            $target
        }
    };
}


/// GetQueryById - GET /query/{queryId}
#[tracing::instrument(skip_all)]
async fn get_query_by_id<I, A, E>(
    method: Method,
    host: Host,
    cookies: CookieJar,
    Path(path_params): Path<models::GetQueryByIdPathParams>,
    State(api_impl): State<I>,
) -> Result<Response, StatusCode>
where
    I: AsRef<A> + Send + Sync,
    A: apis::query::QueryApi<E> + Send + Sync,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    #[allow(clippy::redundant_closure)]
    let validation = tokio::task::spawn_blocking(move || get_query_by_id_validation(path_params))
        .await
        .unwrap();

    let (path_params,) = or_bad_request!(validation);

    let result = api_impl
        .as_ref()
        .get_query_by_id(&method, &host, &cookies, &path_params)
        .await;

    let resp = match result {
        Ok(rsp) => match rsp {
            apis::query::GetQueryByIdResponse::Status200(body) =>
                generic_json_response(200, body).await?,
            apis::query::GetQueryByIdResponse::Status404(body) =>
                generic_json_response(404, body).await?,
        },
        Err(why) => {
            return api_impl
                .as_ref()
                .handle_error(&method, &host, &cookies, why)
                .await;
        }
    };

    resp.map_err(|e| {
        error!(error = ?e);
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[tracing::instrument(skip_all)]
fn get_query_error_validation(
    path_params: models::GetQueryErrorPathParams,
) -> std::result::Result<(models::GetQueryErrorPathParams,), ValidationErrors> {
    path_params.validate()?;

    Ok((path_params,))
}

/// GetQueryError - GET /error/{queryId}
#[tracing::instrument(skip_all)]
async fn get_query_error<I, A, E>(
    method: Method,
    host: Host,
    cookies: CookieJar,
    Path(path_params): Path<models::GetQueryErrorPathParams>,
    State(api_impl): State<I>,
) -> Result<Response, StatusCode>
where
    I: AsRef<A> + Send + Sync,
    A: apis::query::QueryApi<E> + Send + Sync,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    #[allow(clippy::redundant_closure)]
    let validation = tokio::task::spawn_blocking(move || get_query_error_validation(path_params))
        .await
        .unwrap();

    let (path_params,) = or_bad_request!(validation);

    let result = api_impl
        .as_ref()
        .get_query_error(&method, &host, &cookies, &path_params)
        .await;

    let mut response = Response::builder();

    let resp = match result {
        Ok(rsp) => match rsp {
            apis::query::GetQueryErrorResponse::Status200(body) => 
                generic_json_response(200, body).await?,
            apis::query::GetQueryErrorResponse::Status404(body) =>
                generic_json_response(404, body).await?,
            apis::query::GetQueryErrorResponse::Status400(body) =>
                generic_json_response(400, body).await?,
        },
        Err(why) => {
            return api_impl.as_ref().handle_error(&method, &host, &cookies, why).await;
        },
    };

    resp.map_err(|e| {
        error!(error = ?e);
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[derive(validator::Validate)]
#[allow(dead_code)]
struct GetQueryResultBodyValidator<'a> {
    #[validate(nested)]
    body: &'a models::GetQueryResultRequest,
}

#[tracing::instrument(skip_all)]
fn get_query_result_validation(
    path_params: models::GetQueryResultPathParams,
    body: Option<models::GetQueryResultRequest>,
) -> std::result::Result<
    (
        models::GetQueryResultPathParams,
        Option<models::GetQueryResultRequest>,
    ),
    ValidationErrors,
> {
    path_params.validate()?;
    if let Some(body) = &body {
        let b = GetQueryResultBodyValidator { body };
        b.validate()?;
    }

    Ok((path_params, body))
}

/// GetQueryResult - GET /result/{queryId}
#[tracing::instrument(skip_all)]
async fn get_query_result<I, A, E>(
    method: Method,
    host: Host,
    cookies: CookieJar,
    Path(path_params): Path<models::GetQueryResultPathParams>,
    State(api_impl): State<I>,
    Json(body): Json<Option<models::GetQueryResultRequest>>,
) -> Result<Response, StatusCode>
where
    I: AsRef<A> + Send + Sync,
    A: apis::query::QueryApi<E> + Send + Sync,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    #[allow(clippy::redundant_closure)]
    let validation =
        tokio::task::spawn_blocking(move || get_query_result_validation(path_params, body))
            .await
            .unwrap();

    let (path_params, body) = or_bad_request!(validation, (path_params, body));

    let result = api_impl
        .as_ref()
        .get_query_result(&method, &host, &cookies, &path_params, &body)
        .await;

    let mut response = Response::builder();

    let resp = match result {
        Ok(rsp) => match rsp {
            apis::query::GetQueryResultResponse::Status200(body) =>
                generic_json_response(200, body).await?,
            apis::query::GetQueryResultResponse::Status404(body) =>
                generic_json_response(404, body).await?,
            apis::query::GetQueryResultResponse::Status400(body) =>
                generic_json_response(400, body).await?,
        },
        Err(why) => {
            return api_impl
                .as_ref()
                .handle_error(&method, &host, &cookies, why)
                .await;
        }
    };

    resp.map_err(|e| {
        error!(error = ?e);
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[derive(validator::Validate)]
#[allow(dead_code)]
struct SubmitQueryBodyValidator<'a> {
    #[validate(nested)]
    body: &'a models::ExecuteQueryRequest,
}

#[tracing::instrument(skip_all)]
fn submit_query_validation(
    body: models::ExecuteQueryRequest,
) -> std::result::Result<(models::ExecuteQueryRequest,), ValidationErrors> {
    let b = SubmitQueryBodyValidator { body: &body };
    b.validate()?;

    Ok((body,))
}

/// SubmitQuery - POST /query
#[tracing::instrument(skip_all)]
async fn submit_query<I, A, E>(
    method: Method,
    host: Host,
    cookies: CookieJar,
    State(api_impl): State<I>,
    Json(body): Json<models::ExecuteQueryRequest>,
) -> Result<Response, StatusCode>
where
    I: AsRef<A> + Send + Sync,
    A: apis::query::QueryApi<E> + Send + Sync,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    #[allow(clippy::redundant_closure)]
    let validation = tokio::task::spawn_blocking(move || submit_query_validation(body))
        .await
        .unwrap();

    let (body,) = or_bad_request!(validation);

    let result = api_impl
        .as_ref()
        .submit_query(&method, &host, &cookies, &body)
        .await;

    let mut response = Response::builder();

    let resp = match result {
        Ok(rsp) => match rsp {
            apis::query::SubmitQueryResponse::Status200(body) =>
                generic_json_response(200, body).await?,
            apis::query::SubmitQueryResponse::Status400(body) =>
                generic_json_response(400, body).await?,
        },
        Err(why) => {
            return api_impl.as_ref().handle_error(&method, &host, &cookies, why).await;
        },
    };

    resp.map_err(|e| {
        error!(error = ?e);
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[tracing::instrument(skip_all)]
fn get_queries_validation() -> std::result::Result<(), ValidationErrors> {
    Ok(())
}

/// GetQueries - GET /queries
#[tracing::instrument(skip_all)]
async fn get_queries<I, A, E>(
    method: Method,
    host: Host,
    cookies: CookieJar,
    State(api_impl): State<I>,
) -> Result<Response, StatusCode>
where
    I: AsRef<A> + Send + Sync,
    A: apis::query::QueryApi<E> + Send + Sync,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    #[allow(clippy::redundant_closure)]
    let validation = tokio::task::spawn_blocking(move || get_queries_validation())
        .await
        .unwrap();

    let _ = or_bad_request!(validation, ());

    let result = api_impl
        .as_ref()
        .get_queries(&method, &host, &cookies)
        .await;

    let mut response = Response::builder();

    let resp = match result {
        Ok(rsp) => match rsp {
            apis::query::GetQueriesResponse::Status200(body) =>
                generic_json_response(200, body).await?,
        },
        Err(why) => {
            return api_impl.as_ref().handle_error(&method, &host, &cookies, why).await;
        },
    };

    resp.map_err(|e| {
        error!(error = ?e);
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[tracing::instrument(skip_all)]
fn get_system_info_validation() -> std::result::Result<(), ValidationErrors> {
    Ok(())
}

/// GetSystemInfo - GET /system/info
#[tracing::instrument(skip_all)]
async fn get_system_info<I, A, E>(
    method: Method,
    host: Host,
    cookies: CookieJar,
    State(api_impl): State<I>,
) -> Result<Response, StatusCode>
where
    I: AsRef<A> + Send + Sync,
    A: apis::metadata::MetadataApi<E> + Send + Sync,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    #[allow(clippy::redundant_closure)]
    let validation = tokio::task::spawn_blocking(move || get_system_info_validation())
        .await
        .unwrap();

    let _ = or_bad_request!(validation, ());

    let result = api_impl
        .as_ref()
        .get_system_info(&method, &host, &cookies)
        .await;

    let mut response = Response::builder();

    let resp = match result {
        Ok(rsp) => match rsp {
            apis::metadata::GetSystemInfoResponse::Status200(body) =>
                generic_json_response(200, body).await?,
        },
        Err(why) => {
            return api_impl.as_ref().handle_error(&method, &host, &cookies, why).await;
        },
    };

    resp.map_err(|e| {
        error!(error = ?e);
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[derive(validator::Validate)]
#[allow(dead_code)]
struct CreateTableBodyValidator<'a> {
    #[validate(nested)]
    body: &'a models::TableSchema,
}

#[tracing::instrument(skip_all)]
fn create_table_validation(
    body: models::TableSchema,
) -> std::result::Result<(models::TableSchema,), ValidationErrors> {
    let b = CreateTableBodyValidator { body: &body };
    b.validate()?;

    Ok((body,))
}

/// CreateTable - PUT /table
#[tracing::instrument(skip_all)]
async fn create_table<I, A, E>(
    method: Method,
    host: Host,
    cookies: CookieJar,
    State(api_impl): State<I>,
    Json(body): Json<models::TableSchema>,
) -> Result<Response, StatusCode>
where
    I: AsRef<A> + Send + Sync,
    A: apis::schema::SchemaApi<E> + Send + Sync,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    #[allow(clippy::redundant_closure)]
    let validation = tokio::task::spawn_blocking(move || create_table_validation(body))
        .await
        .unwrap();

    let (body,) = or_bad_request!(validation);

    let result = api_impl
        .as_ref()
        .create_table(&method, &host, &cookies, &body)
        .await;

    let mut response = Response::builder();

    let resp = match result {
        Ok(rsp) => match rsp {
            apis::schema::CreateTableResponse::Status200(body) =>
                generic_json_response(200, body).await?,
            apis::schema::CreateTableResponse::Status400(body) =>
                generic_json_response(400, body).await?,
        },
        Err(why) => {
            return api_impl.as_ref().handle_error(&method, &host, &cookies, why).await;
        },
    };

    resp.map_err(|e| {
        error!(error = ?e);
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[tracing::instrument(skip_all)]
fn delete_table_validation(
    path_params: models::DeleteTablePathParams,
) -> std::result::Result<(models::DeleteTablePathParams,), ValidationErrors> {
    path_params.validate()?;

    Ok((path_params,))
}

/// DeleteTable - DELETE /table/{tableId}
#[tracing::instrument(skip_all)]
async fn delete_table<I, A, E>(
    method: Method,
    host: Host,
    cookies: CookieJar,
    Path(path_params): Path<models::DeleteTablePathParams>,
    State(api_impl): State<I>,
) -> Result<Response, StatusCode>
where
    I: AsRef<A> + Send + Sync,
    A: apis::schema::SchemaApi<E> + Send + Sync,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    #[allow(clippy::redundant_closure)]
    let validation = tokio::task::spawn_blocking(move || delete_table_validation(path_params))
        .await
        .unwrap();

    let (path_params,) = or_bad_request!(validation);

    let result = api_impl
        .as_ref()
        .delete_table(&method, &host, &cookies, &path_params)
        .await;

    let mut response = Response::builder();

    let resp = match result {
        Ok(rsp) => match rsp {
            apis::schema::DeleteTableResponse::Status200 => {
                let mut response = response.status(200);
                response.body(Body::empty())
            }
            apis::schema::DeleteTableResponse::Status404(body) =>
                generic_json_response(404, body).await?,
        },
        Err(why) => {
            return api_impl
                .as_ref()
                .handle_error(&method, &host, &cookies, why)
                .await;
        }
    };

    resp.map_err(|e| {
        error!(error = ?e);
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[tracing::instrument(skip_all)]
fn get_table_by_id_validation(
    path_params: models::GetTableByIdPathParams,
) -> std::result::Result<(models::GetTableByIdPathParams,), ValidationErrors> {
    path_params.validate()?;

    Ok((path_params,))
}

/// GetTableById - GET /table/{tableId}
#[tracing::instrument(skip_all)]
async fn get_table_by_id<I, A, E>(
    method: Method,
    host: Host,
    cookies: CookieJar,
    Path(path_params): Path<models::GetTableByIdPathParams>,
    State(api_impl): State<I>,
) -> Result<Response, StatusCode>
where
    I: AsRef<A> + Send + Sync,
    A: apis::schema::SchemaApi<E> + Send + Sync,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    #[allow(clippy::redundant_closure)]
    let validation = tokio::task::spawn_blocking(move || get_table_by_id_validation(path_params))
        .await
        .unwrap();

    let (path_params,) = or_bad_request!(validation);

    let result = api_impl
        .as_ref()
        .get_table_by_id(&method, &host, &cookies, &path_params)
        .await;

    let mut response = Response::builder();

    let resp = match result {
        Ok(rsp) => match rsp {
            apis::schema::GetTableByIdResponse::Status200(body) =>
                generic_json_response(200, body).await?,
            apis::schema::GetTableByIdResponse::Status404(body) =>
                generic_json_response(404, body).await?,
        },
        Err(why) => {
            return api_impl
                .as_ref()
                .handle_error(&method, &host, &cookies, why)
                .await;
        }
    };

    resp.map_err(|e| {
        error!(error = ?e);
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[tracing::instrument(skip_all)]
fn get_tables_validation() -> std::result::Result<(), ValidationErrors> {
    Ok(())
}

/// GetTables - GET /tables
#[tracing::instrument(skip_all)]
async fn get_tables<I, A, E>(
    method: Method,
    host: Host,
    cookies: CookieJar,
    State(api_impl): State<I>,
) -> Result<Response, StatusCode>
where
    I: AsRef<A> + Send + Sync,
    A: apis::schema::SchemaApi<E> + Send + Sync,
    E: std::fmt::Debug + Send + Sync + 'static,
{
    #[allow(clippy::redundant_closure)]
    let validation = tokio::task::spawn_blocking(move || get_tables_validation())
        .await
        .unwrap();

    let _ = or_bad_request!(validation, ());

    let result = api_impl.as_ref().get_tables(&method, &host, &cookies).await;

    let mut response = Response::builder();

    let resp = match result {
        Ok(rsp) => match rsp {
            apis::schema::GetTablesResponse::Status200(body) =>
                generic_json_response(200, body).await?,
        },
        Err(why) => {
            return api_impl
                .as_ref()
                .handle_error(&method, &host, &cookies, why)
                .await;
        }
    };

    resp.map_err(|e| {
        error!(error = ?e);
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[allow(dead_code)]
#[inline]
fn response_with_status_code_only(code: StatusCode) -> Result<Response, StatusCode> {
    Response::builder()
        .status(code)
        .body(Body::empty())
        .map_err(|_| code)
}
