use std::{path::{Path, PathBuf}, string::ToString};
use std::io::Write;
use std::fs::{create_dir_all, create_dir, File};
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::to_string;

const SCHEMA_FILENAME: &'static str = "schema.json";
const DATA_DIRNAME: &'static str = "data";

fn validate_name(name: &str) -> bool {
    let mut started = false;

    for ch in name.chars() {
        if !started && ch.is_ascii_digit() {
            return false
        }
        started = true;

        if !(ch.is_ascii_digit() || ch.is_ascii_lowercase() || ch.is_ascii_uppercase() || ch == '_') {
            return false
        }
    }

    true
}

fn generate_column_filename(table_name: &str, column_name: &str, num: u64) -> String {
    String::from(table_name) + ":" + column_name + ":" + &num.to_string()
}

fn get_column_path(data_path: &Path, column_filename: &str) -> PathBuf {
    data_path.join(DATA_DIRNAME).join(column_filename)
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum ColumnType {
    INT64,
    VARCHAR,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Column {
    pub name: String,
    #[serde(rename="type")]
    pub column_type: ColumnType,
    files: Vec<String>,
}

impl Column {
    pub fn get_file_paths(&self, data_path: &Path) -> Vec<PathBuf> {
        self.files.iter().map(|s| get_column_path(data_path, &s)).collect()
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Table {
    pub columns: Vec<Column>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Schema {
    tables: HashMap<String, Table>
}

#[derive(Debug)]
pub enum TableError {
    TableExists(String),
    InvalidTableName(String),
    UnknownTable(String),
    InvalidColumnName(Vec<String>),
    NoColumns(String),
}

impl Schema {
    pub fn new() -> Schema {
        Schema { tables: HashMap::new() }
    }

    pub fn get_tables(&self) -> &HashMap<String, Table> {
        &self.tables
    }

    pub fn create_table(&mut self, name: String, columns: &[(String, ColumnType)]) -> Result<&mut Self, TableError> {
        if !validate_name(&name) {
            return Err(TableError::InvalidTableName(name.into()))
        }

        if columns.len() == 0 {
            return Err(TableError::NoColumns(name.into()))
        }

        if self.tables.contains_key(&name) {
            return Err(TableError::TableExists(name.into()))
        }

        let invalid_columns: Vec<String> = columns.iter()
            .map(|x| (&x.0, validate_name(&x.0)))
            .filter(|x| !x.1)
            .map(|x| x.0.clone())
            .collect();

        if invalid_columns.len() > 0 {
            return Err(TableError::InvalidColumnName(invalid_columns))
        }

        self.tables.insert(name.into(), Table {
            columns: columns.iter().map(|c| Column {
                name: c.0.clone(),
                column_type: c.1,
                files: vec![],
            }).collect()
        });

        Ok(self)
    }

    pub fn delete_table(&mut self, name: String) -> Result<&mut Self, TableError> {
        self.tables.remove_entry(&name).map_or(Err(TableError::UnknownTable(name)), |_| Ok(self))
    }

    pub fn add_file(&mut self, table_name: String) -> Result<&mut Self, TableError> {
        if let Some(table) = self.tables.get_mut(&table_name) {
            for col in table.columns.iter_mut() {
                col.files.push(generate_column_filename(
                    &table_name,
                    &col.name,
                    col.files.len().try_into().unwrap(),
                ));
            }
            Ok(self)
        } else {
            Err(TableError::UnknownTable(table_name.into()))
        }

    }
}

impl TryFrom<&[u8]> for Schema {
    type Error = serde_json::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        serde_json::from_slice(value)
    }
}

impl TryInto<String> for &Schema {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<String, Self::Error> {
        serde_json::to_string(&self)
    }
}

pub fn get_schema_path(data_path: &Path) -> PathBuf {
    data_path.join(SCHEMA_FILENAME)
}

pub fn create_schema_dir(data_path: &Path) -> std::io::Result<()> {
    create_dir_all(data_path.join(DATA_DIRNAME))?;

    let mut file = File::options()
        .create(true)
        .write(true)
        .truncate(true)
        .open(data_path.join(SCHEMA_FILENAME))?;

    file.write(to_string(&Schema {
        tables: HashMap::new(),
    })?.as_bytes())?;

    Ok(())
}
