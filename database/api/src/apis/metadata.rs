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
pub enum GetSystemInfoResponse {
    /// Basic information about the system
    Status200(models::SystemInformation),
}

#[async_trait]
#[allow(clippy::ptr_arg)]
pub trait MetadataApi<E: std::fmt::Debug + Send + Sync + 'static = ()>:
    super::ErrorHandler<E>
{
    /// Get basic information about the system (e.g. version, uptime, etc.).
    ///
    /// GetSystemInfo - GET /system/info
    async fn get_system_info(
        &self,
        method: &Method,
        host: &Host,
        cookies: &CookieJar,
    ) -> Result<GetSystemInfoResponse, E>;
}
