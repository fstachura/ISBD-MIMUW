use async_trait::async_trait;
use axum::extract::*;
use axum_extra::extract::{CookieJar, Host};
use bytes::Bytes;
use http::Method;
use serde::{Deserialize, Serialize};

use crate::models;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum CreateTableResponse {
    /// Table created successfully
    Status200(String),
    /// Response used when more problems can occur in the system when processing request
    Status400(models::MultipleProblemsError),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum DeleteTableResponse {
    /// Table has been deleted successfully
    Status200,
    /// Generic error
    Status404(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum GetTableByIdResponse {
    /// Detailed Table description
    Status200(models::TableSchema),
    /// Generic error
    Status404(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub enum GetTablesResponse {
    /// Array of tables in database
    Status200(Vec<models::ShallowTable>),
}

/// Proj3Schema
#[async_trait]
#[allow(clippy::ptr_arg)]
pub trait SchemaApi<E: std::fmt::Debug + Send + Sync + 'static = ()>:
    super::ErrorHandler<E>
{
    /// Create new table in database.
    ///
    /// CreateTable - PUT /table
    async fn create_table(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        body: &models::TableSchema,
    ) -> Result<CreateTableResponse, E>;

    /// Delete selected table from database.
    ///
    /// DeleteTable - DELETE /table/{tableId}
    async fn delete_table(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        path_params: &models::DeleteTablePathParams,
    ) -> Result<DeleteTableResponse, E>;

    /// Get detailed description of selected table.
    ///
    /// GetTableById - GET /table/{tableId}
    async fn get_table_by_id(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
        path_params: &models::GetTableByIdPathParams,
    ) -> Result<GetTableByIdResponse, E>;

    /// Get list of tables with their accompanying IDs. Use those IDs to get details by calling /table endpoint..
    ///
    /// GetTables - GET /tables
    async fn get_tables(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
    ) -> Result<GetTablesResponse, E>;
}
