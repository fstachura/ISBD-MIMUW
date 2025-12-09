use tokio::{fs::{File, read_dir, remove_file}, sync::{Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, RwLockReadGuard}, task::spawn_blocking};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use std::{collections::HashMap, error::Error, io::{self, Read, Seek, SeekFrom, Write}, path::PathBuf, sync::{Arc, atomic::AtomicBool}};
use schema::{ColumnType, Table, TableError};

#[derive(Debug)]
pub struct LockedTable {
    pub table: Table,
    pub data_dir: PathBuf,
    deleted: bool,
}

#[derive(Clone, Debug)]
pub struct TableManager {
    table: Arc<RwLock<LockedTable>>,
    schema_path: PathBuf,
    copy_mutex: Arc<Mutex<()>>,
}

pub struct TableLockedForCopy {
    #[allow(dead_code)]
    copy_token: OwnedMutexGuard<()>,
    read_token: OwnedRwLockReadGuard<LockedTable>,
    table: Arc<RwLock<LockedTable>>,
    copy_targets: Vec<(String, ColumnType, String)>,
    schema_path: PathBuf,
}

#[derive(Debug)]
pub enum FinishWriteError {
    TableDeleted,
    InvalidFilenames,
    IoError(String, std::io::Error),
}

impl TableLockedForCopy {
    pub fn table(&self) -> Option<(&Table, &PathBuf)> {
        if self.read_token.deleted {
            None    
        } else {
            Some((&self.read_token.table, &self.read_token.data_dir))
        }
    }

    pub fn copy_targets(&self) -> &Vec<(String, ColumnType, String)> {
        &self.copy_targets
    }

    pub async fn finish_write(self) -> Result<(), FinishWriteError> {
        drop(self.read_token);
        let mut write_lock = self.table.write().await;
        if !write_lock.deleted {
            if let Ok(()) = write_lock.table.add_filenames(self.copy_targets) {
                let mut file = File::options()
                    .write(true)
                    .truncate(false)
                    .create(false)
                    .open(self.schema_path)
                    .await
                    .map_err(|e| FinishWriteError::IoError("failed to open table file".into(), e))?;

                flush_table(&mut file, &write_lock.table)
                    .await
                    .map_err(|e| FinishWriteError::IoError("failed to write table file".into(), e))?;
                Ok(())
            } else {
                Err(FinishWriteError::InvalidFilenames)
            }
        } else {
            Err(FinishWriteError::TableDeleted)
        }
    }
}

pub struct TableLockedForSelect {
    read_token: OwnedRwLockReadGuard<LockedTable>,
}

impl TableLockedForSelect {
    pub fn table(&self) -> Option<(&Table, &PathBuf)> {
        if self.read_token.deleted {
            None    
        } else {
            Some((&self.read_token.table, &self.read_token.data_dir))
        }
    }
}

impl TableManager {
    fn new(schema_path: PathBuf, data_dir: PathBuf, table: Table) -> Self {
        TableManager { 
            table: Arc::new(RwLock::new(LockedTable {
                data_dir,
                table,
                deleted: false,
            })), 
            schema_path,
            copy_mutex: Arc::new(Mutex::new(())),
        }
    }

    pub async fn lock_select(&self) -> TableLockedForSelect {
        TableLockedForSelect {
            read_token: self.table.clone().read_owned().await,
        }
    }

    pub async fn lock_copy(&self) -> TableLockedForCopy {
        let copy_token = self.copy_mutex.clone().lock_owned().await;
        let read_token = self.table.clone().read_owned().await;
        let copy_targets = read_token.table.new_filenames();
        TableLockedForCopy {
            copy_token, 
            read_token,
            table: self.table.clone(),
            schema_path: self.schema_path.clone(),
            copy_targets,
        }
    }

    pub async fn read_schema<'a>(&'a self) -> Option<RwLockReadGuard<'a, LockedTable>> {
        let table = self.table.read().await;
        if table.deleted {
            None
        } else {
            Some(table)
        }
    }
}

async fn flush_table(file: &mut File, table: &Table) -> std::io::Result<()> {
    let data: String = table.try_into()?;
    file.seek(std::io::SeekFrom::Start(0)).await?;
    file.write(&data.as_bytes()).await?;
    file.sync_all().await?;
    Ok(())
}

#[derive(Clone)]
pub struct SchemaManager {
    data_dir: PathBuf,
    schema_dir: PathBuf,
    schema: Arc<RwLock<HashMap<String, TableManager>>>,
}

#[derive(Debug)]
pub enum SchemaError {
    TableExists(String),
    UnknownTable(String),
    TableError(TableError),
    SerdeError(serde_json::Error),
    IoError(String, std::io::Error),
}

impl SchemaManager {
    pub async fn new(schema_dir: PathBuf, data_dir: PathBuf) -> Result<Self, std::io::Error> {
        let mut schema_map = HashMap::new();

        let mut dir = read_dir(schema_dir.clone()).await?;
        while let Some(entry) = dir.next_entry().await? {
            let filename = entry.file_name();
            let filename = filename.to_str()
                .ok_or(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("failed to convert schema filename {:?}", entry.file_name())
                ))?;

            if filename.ends_with(".json") {
                let mut file = File::open(entry.path()).await?;
                let mut buf = vec![];
                file.read_to_end(&mut buf).await?;
                let table: Table = buf.as_slice().try_into()
                    .map_err(|e: <Table as TryFrom<&[u8]>>::Error| Into::<std::io::Error>::into(e))?;

                schema_map.insert(
                    table.name().clone(),
                    TableManager::new(entry.path(), data_dir.clone(), table)
                );
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("non-json file found in schema directory {:?}", entry.file_name())
                ));
            }
        }

        Ok(SchemaManager {
            data_dir,
            schema_dir,
            schema: Arc::new(RwLock::new(schema_map)),
        })
    }

    pub async fn create_table(&self, name: String, columns: &[(String, ColumnType)]) ->
        Result<TableManager, SchemaError> {

        // NOTE: assuming table checks if name is valid
        let table = Table::new(name.clone(), columns)
            .map_err(SchemaError::TableError)?;

        let schema_path = self.schema_dir
            .join(name.clone() + ".json");

        let table_json: String = (&table)
            .try_into()
            .map_err(|v| SchemaError::SerdeError(v))?;

        let manager = TableManager::new(
            schema_path.clone(),
            self.data_dir.clone(),
            table
        );

        let mut schema = self.schema.write().await;

        if schema.contains_key(&name) {
            return Err(SchemaError::TableExists(name));
        }

        let mut file = File::options()
            .create_new(true) // we have write on schema - table is not being deleted right now
            .truncate(true)
            .write(true)
            .open(schema_path)
            .await
            .map_err(|e|
                SchemaError::IoError("failed to create schema file".into(), e)
            )?;

        schema.insert(name.clone(), manager.clone());

        if let Err(err) = file.write_all(table_json.as_bytes()).await {
            schema.remove(&name);
            Err(SchemaError::IoError("failed to write schema file".into(), err))
        } else {
            Ok(manager)
        }
    }

    pub async fn delete_table(&self, name: &str) -> Result<(), SchemaError> {
        let mut schema = self.schema.write().await;

        let mut table_manager = schema.remove(name)
            .ok_or(SchemaError::UnknownTable(name.to_string()))?;

        if let Err(err) = remove_file(table_manager.schema_path.clone()).await {
            schema.insert(name.to_string(), table_manager);
            return Err(SchemaError::IoError("failed to remove schema file".into(), err))
        }

        // no writers can take lock during downgrade. readers can still read schema.
        // adding another table with same name will be impossible as it requires write
        // on schema
        let mut schema = schema.downgrade();

        let mut table = table_manager.table.write().await;
        table.deleted = true;

        for col in table.table.columns() {
            for path in col.get_file_paths(&self.data_dir) {
                if let Err(err) = tokio::fs::remove_file(path.clone()).await {
                    println!("failed to remove column file {:?}", path);
                }
            }
        }

        Ok(())
    }

    pub async fn get_table_manager(&self, name: &str) -> Option<TableManager> {
        self.schema.read().await.get(name).cloned()
    }

    pub async fn list_tables(&self) -> Vec<String> {
        self.schema.read().await.keys().cloned().collect()
    }
}
