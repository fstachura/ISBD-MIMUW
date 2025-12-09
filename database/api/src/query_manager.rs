use std::{collections::HashMap, error::Error, path::PathBuf, sync::Arc};
use tokio::{fs::File, sync::{RwLock, mpsc::{self, Receiver, Sender, error::SendError}}, task::{JoinError, spawn_blocking}};
use uuid::{Uuid};

use crate::query_planner::QueryPlanError;

#[derive(Clone)]
pub enum Query {
    Copy {
        source: PathBuf,
        target: String,
        columns: Option<Vec<String>>,
        contains_header: bool,
    },
    Select {
        table: String,
    },
}

#[derive(Clone, Debug)]
pub enum Column {
    String(Vec<Vec<Vec<String>>>),
    Int64(Vec<Vec<Vec<i64>>>),
}

#[derive(Clone, Debug)]
pub enum QueryResult {
    Copy,
    Select(Arc<Vec<Column>>)
}

#[derive(Clone, Debug)]
pub enum QueryError {
    TableDeleted,
    ExecuteError,
    PlanError(QueryPlanError),
    Unknown(String),
}

#[derive(Clone)]
pub enum QueryStateMarker {
    Created,
    Planning,
    Running,
    Completed(QueryResult),
    Failed(QueryError),
}

#[derive(Clone)]
pub struct QueryManager {
    queries: Arc<RwLock<HashMap<Uuid, (Query, Arc<RwLock<QueryStateMarker>>)>>>,
    query_sender: Sender<(Uuid, Query, Arc<RwLock<QueryStateMarker>>)>,
}

impl QueryManager {
    pub fn new() -> (Self, Receiver<(Uuid, Query, Arc<RwLock<QueryStateMarker>>)>) {
        let (tx, rx) = mpsc::channel::<(Uuid, Query, Arc<RwLock<QueryStateMarker>>)>(16);

        (QueryManager {
            queries: Arc::new(RwLock::new(HashMap::new())),
            query_sender: tx,
        }, rx)
    }

    pub async fn get_query_state(&self, uuid: &Uuid) -> Option<(Query, Arc<RwLock<QueryStateMarker>>)> {
        self.queries.read().await.get(uuid).map(|v| v.clone())
    }

    pub async fn consume_query(&self, uuid: &Uuid) -> Option<(Query, Arc<RwLock<QueryStateMarker>>)> {
        self.queries.write().await.remove_entry(uuid).map(|v| v.1)
    }

    pub async fn list_queries(&self) -> Result<Vec<(Uuid, QueryStateMarker)>, JoinError> {
        let queries: Vec<_> = self.queries.read().await.iter().map(|v| (v.0.clone(), v.1.clone())).collect();
        spawn_blocking(move || {
            queries.iter().map(|v| (v.0, v.1.1.blocking_read().clone())).collect()
        }).await
    }

    pub async fn submit_query(&self, query: Query)
        -> Result<(Uuid, Arc<RwLock<QueryStateMarker>>), SendError<(Uuid, Query, Arc<RwLock<QueryStateMarker>>)>>
    {
        let id = Uuid::new_v4();
        let state_marker = Arc::new(RwLock::new(QueryStateMarker::Created));
        self.queries.write().await.insert(
            id,
            (query.clone(), state_marker.clone())
        );

        if let Err(err) = self.query_sender.send((id, query, state_marker.clone())).await {
            self.queries.write().await.remove(&id);
            Err(err)
        } else {
            Ok((id, state_marker))
        }
    }
}
