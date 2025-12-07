#![allow(unused_qualifications)]

use std::sync::Arc;

use validator::Validate;

use crate::{models};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct GetQueryByIdPathParams {
    /// ID of selected Query
    pub query_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct GetQueryErrorPathParams {
    /// ID of selected Query
    pub query_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct GetQueryResultPathParams {
    /// ID of selected Query
    pub query_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct DeleteTablePathParams {
    /// ID of selected Table
    pub table_id: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct GetTableByIdPathParams {
    /// ID of selected Table
    pub table_id: String,
}

/// Description of single column in table
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Column {
    #[serde(rename = "name")]
    pub name: String,

    #[serde(rename = "type")]
    #[validate(nested)]
    pub r_type: models::LogicalColumnType,
}

/// Description of the COPY query from CSV file. Server will read the file and insert all data into selected table.
/// When number of columns in source and target doesn't match, user have to use \"destinationColumns\" property to specify which columns data should be inserted into.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct CopyQuery {
    /// Path to source CSV file (filepath in perspective of running server! NOT client)
    #[serde(rename = "sourceFilepath")]
    pub source_filepath: String,

    #[serde(rename = "destinationTableName")]
    pub destination_table_name: String,

    /// List of columns to copy data into. It creates a map from source columns to destination columns. Assumes that data in source file is in the same order as in this list.
    #[serde(rename = "destinationColumns")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_columns: Option<Vec<String>>,

    /// Whether CSV file contains header row
    #[serde(rename = "doesCsvContainHeader")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub does_csv_contain_header: Option<bool>,
}

/// Generic error object
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Error {
    #[serde(rename = "message")]
    pub message: String,
}

/// Used to submit a new query for execution
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ExecuteQueryRequest {
    #[serde(rename = "queryDefinition")]
    #[validate(nested)]
    pub query_definition: models::QueryQueryDefinition,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct GetQueryResultRequest {
    /// Maximum number of rows to return
    #[serde(rename = "rowLimit")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_limit: Option<usize>,

    /// Say to system that result will not be accessed by the user anymore (it is safe to release the resources connected with the result)
    #[serde(rename = "flushResult")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_result: Option<bool>,
}

/// Enum describing logical column types
/// Enumeration of values.
/// Since this enum's variants do not hold data, we can easily define them as `#[repr(C)]`
/// which helps with FFI.
#[allow(non_camel_case_types, clippy::large_enum_variant)]
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "conversion", derive(frunk_enum_derive::LabelledGenericEnum))]
pub enum LogicalColumnType {
    #[serde(rename = "INT64")]
    Int64,
    #[serde(rename = "VARCHAR")]
    Varchar,
}

impl validator::Validate for LogicalColumnType {
    fn validate(&self) -> std::result::Result<(), validator::ValidationErrors> {
        std::result::Result::Ok(())
    }
}

/// Error containing multiple problems about request processing. Useful when processing complex requests where multiple problems can occur at the same time.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct MultipleProblemsError {
    #[serde(rename = "problems")]
    #[validate(nested)]
    pub problems: Vec<models::MultipleProblemsErrorProblemsInner>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct MultipleProblemsErrorProblemsInner {
    /// Description of particular problem
    #[serde(rename = "error")]
    pub error: String,

    /// Optional context for user (for e.g. which column caused problem). It can be helpful for troubleshooting.
    #[serde(rename = "context")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
}

/// Description of a query in the system
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct Query {
    /// ID of selected Query (I propose UUID, but it is under your own discretion)
    #[serde(rename = "queryId")]
    pub query_id: String,

    #[serde(rename = "status")]
    #[validate(nested)]
    pub status: models::QueryStatus,

    /// Whether result of this query is already available
    #[serde(rename = "isResultAvailable")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_result_available: Option<bool>,

    #[serde(rename = "queryDefinition")]
    #[validate(nested)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_definition: Option<models::QueryQueryDefinition>,
}

/// ID of selected Query (I propose UUID, but it is under your own discretion)
#[derive(Debug, Clone, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct QueryId(pub String);

impl validator::Validate for QueryId {
    fn validate(&self) -> std::result::Result<(), validator::ValidationErrors> {
        std::result::Result::Ok(())
    }
}

impl std::convert::From<String> for QueryId {
    fn from(x: String) -> Self {
        QueryId(x)
    }
}

impl std::convert::From<QueryId> for String {
    fn from(x: QueryId) -> Self {
        x.0
    }
}

impl std::ops::Deref for QueryId {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}

impl std::ops::DerefMut for QueryId {
    fn deref_mut(&mut self) -> &mut String {
        &mut self.0
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
#[allow(non_camel_case_types, clippy::large_enum_variant)]
pub enum QueryQueryDefinition {
    SelectQuery(models::SelectQuery),
    CopyQuery(models::CopyQuery),
}

impl validator::Validate for QueryQueryDefinition {
    fn validate(&self) -> std::result::Result<(), validator::ValidationErrors> {
        match self {
            Self::SelectQuery(v) => v.validate(),
            Self::CopyQuery(v) => v.validate(),
        }
    }
}

/// Converts Query Parameters representation (style=form, explode=false) to a QueryQueryDefinition value
/// as specified in https://swagger.io/docs/specification/serialization/
/// Should be implemented in a serde deserializer
impl std::str::FromStr for QueryQueryDefinition {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}

impl From<models::SelectQuery> for QueryQueryDefinition {
    fn from(value: models::SelectQuery) -> Self {
        Self::SelectQuery(value)
    }
}
impl From<models::CopyQuery> for QueryQueryDefinition {
    fn from(value: models::CopyQuery) -> Self {
        Self::CopyQuery(value)
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct QueryResultInner {
    /// Number of rows in result
    #[serde(rename = "rowCount")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<usize>,

    /// Array of columns in result (all should have the same length equal to rowCount)
    #[serde(rename = "columns")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<models::QueryResultInnerColumnsInner>>,
}

// TODO: rc feature - is this what i want?
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
#[allow(non_camel_case_types, clippy::large_enum_variant)]
pub enum QueryResultInnerColumnsInner {
    VecOfi64(Arc<Vec<i64>>),
    VecOfString(Arc<Vec<String>>),
}

/// Enum describing possible query statuses
/// Enumeration of values.
/// Since this enum's variants do not hold data, we can easily define them as `#[repr(C)]`
/// which helps with FFI.
#[allow(non_camel_case_types, clippy::large_enum_variant)]
#[repr(C)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[cfg_attr(feature = "conversion", derive(frunk_enum_derive::LabelledGenericEnum))]
pub enum QueryStatus {
    #[serde(rename = "CREATED")]
    Created,
    #[serde(rename = "PLANNING")]
    Planning,
    #[serde(rename = "RUNNING")]
    Running,
    #[serde(rename = "COMPLETED")]
    Completed,
    #[serde(rename = "FAILED")]
    Failed,
}

impl validator::Validate for QueryStatus {
    fn validate(&self) -> std::result::Result<(), validator::ValidationErrors> {
        std::result::Result::Ok(())
    }
}

/// Description of a select query (extension in project no 4)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SelectQuery {
    #[serde(rename = "tableName")]
    pub table_name: String,
}

/// Description of a shallow representation of a query
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ShallowQuery {
    /// ID of selected Query (I propose UUID, but it is under your own discretion)
    #[serde(rename = "queryId")]
    pub query_id: String,

    #[serde(rename = "status")]
    #[validate(nested)]
    pub status: models::QueryStatus,
}

/// Description of a shallow representation of a table (e.g. without detailed column information)
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct ShallowTable {
    /// ID of selected Table (I propose UUID, but it is under your own discretion)
    #[serde(rename = "tableId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_id: Option<String>,

    #[serde(rename = "name")]
    pub name: String,
}

/// Basic information about the system
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct SystemInformation {
    /// Version of the DBMS interface
    #[serde(rename = "interfaceVersion")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interface_version: Option<String>,

    /// Version of the DBMS system
    #[serde(rename = "version")]
    pub version: String,

    /// Author of the DBMS system (will help me to automate testing)
    #[serde(rename = "author")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,

    /// System uptime in seconds
    #[serde(rename = "uptime")]
    pub uptime: u64,
}

/// Description of the table in the database
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, validator::Validate)]
#[cfg_attr(feature = "conversion", derive(frunk::LabelledGeneric))]
pub struct TableSchema {
    #[serde(rename = "name")]
    pub name: String,

    #[serde(rename = "columns")]
    #[validate(nested)]
    pub columns: Vec<models::Column>,
}
