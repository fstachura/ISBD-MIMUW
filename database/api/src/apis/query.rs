use async_trait::async_trait;
use axum::extract::*;
use axum_extra::extract::{CookieJar, Host};
use bytes::Bytes;
use http::Method;
use serde::{Deserialize, Serialize};

use crate::{models};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum GetQueriesResponse {
    /// Array of queries submitted to the system
    Status200(Vec<models::ShallowQuery>),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum GetQueryByIdResponse {
    /// Detailed Query description
    Status200(models::Query),
    /// Generic error
    Status404(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum GetQueryErrorResponse {
    /// Response used when more problems can occur in the system when processing request
    Status200(models::MultipleProblemsError),
    /// Generic error
    Status404(models::Error),
    /// Generic error
    Status400(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum GetQueryResultResponse {
    /// Result of selected query
    Status200(Vec<models::QueryResultInner>),
    /// Generic error
    Status404(models::Error),
    /// Generic error
    Status400(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum SubmitQueryResponse {
    /// Query has been created successfully
    Status200(String),
    /// Response used when more problems can occur in the system when processing request
    Status400(models::MultipleProblemsError),
}

#[async_trait]
#[allow(clippy::ptr_arg)]
pub trait QueryApi<E: std::fmt::Debug + Send + Sync + 'static = ()>:
    super::ErrorHandler<E>
{
    /// Get detailed status of selected query.
    ///
    /// GetQueryById - GET /query/{queryId}
    async fn get_query_by_id(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        path_params: &models::GetQueryByIdPathParams,
    ) -> Result<GetQueryByIdResponse, E>;

    /// Get error of selected query (will be available only for queries in FAILED state).
    ///
    /// GetQueryError - GET /error/{queryId}
    async fn get_query_error(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        path_params: &models::GetQueryErrorPathParams,
    ) -> Result<GetQueryErrorResponse, E>;

    /// Get result of selected query (will be available only for SELECT queries after they are completed).
    ///
    /// GetQueryResult - GET /result/{queryId}
    async fn get_query_result(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        path_params: &models::GetQueryResultPathParams,
        body: &Option<models::GetQueryResultRequest>,
    ) -> Result<GetQueryResultResponse, E>;

    /// Submit new query for execution.
    ///
    /// SubmitQuery - POST /query
    async fn submit_query(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        body: &models::ExecuteQueryRequest,
    ) -> Result<SubmitQueryResponse, E>;

    /// Get list of queries (optional in project 3, but useful). Use those IDs to get details by calling /query endpoint..
    ///
    /// GetQueries - GET /queries
    async fn get_queries(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
    ) -> Result<GetQueriesResponse, E>;
}
