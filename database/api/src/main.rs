#![allow(
    missing_docs,
    trivial_casts,
    unused_variables,
    unused_mut,
    unused_extern_crates,
    non_camel_case_types,
    unused_imports,
    unused_attributes
)]
#![allow(
    clippy::derive_partial_eq_without_eq,
    clippy::disallowed_names,
    clippy::too_many_arguments
)]

pub mod api_impl;
pub mod apis;
pub mod consts;
pub mod models;
pub mod query_executor;
pub mod query_manager;
pub mod query_planner;
pub mod schema_manager;
pub mod server;

use schema::ColumnType;
use std::time::Duration;
use std::{env::args, path::Path, process::ExitCode, sync::Arc};
use tokio::{sync::RwLock, time::Instant};
use tracing_subscriber::EnvFilter;

use crate::api_impl::ApiImpl;
use crate::query_executor::start_query_executor;
use crate::query_manager::{QueryManager, QueryResult, QueryStateMarker};
use crate::schema_manager::SchemaManager;

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = args();
    if args.len() < 3 {
        println!("usage: (init|serve) data_dir");
        return ExitCode::FAILURE;
    }

    let _ = args.next().unwrap();
    let cmd = args.next().unwrap();
    let data_dir = args.next().unwrap();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("tower_http=debug"))
                .unwrap(),
        )
        .init();

    match cmd.as_str() {
        "init" => {
            schema::create_schema_dir(Path::new(&data_dir)).unwrap();
            println!("data directory initialized");
            return ExitCode::SUCCESS;
        }
        "serve" => {
            let (qm, query_receiver) = QueryManager::new();
            let sm = SchemaManager::new(
                (data_dir.clone() + "/schema").into(),
                (data_dir + "/data").into(),
            )
            .await
            .expect("failed to create schema manager");

            let qe_handle = start_query_executor(sm.clone(), qm.clone(), query_receiver).await;

            let api = Box::new(ApiImpl {
                start_time: Instant::now(),
                schema_manager: sm,
                query_manager: qm,
            });

            let app = server::new(api);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
                .await
                .unwrap();
            axum::serve(listener, app).await.unwrap();
            qe_handle.await.unwrap();
        }
        _ => {
            println!("invalid cmd. valid cmd: init, serve");
            return ExitCode::FAILURE;
        }
    };

    ExitCode::SUCCESS
}
