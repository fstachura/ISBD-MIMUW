use std::{path::{Path, PathBuf}, string::ToString};
use std::fs::{create_dir_all};
use serde::{Deserialize, Serialize};

const DATA_DIRNAME: &'static str = "data";
const SCHEMA_DIRNAME: &'static str = "schema";

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
    data_path.join(column_filename)
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum ColumnType {
    INT64,
    VARCHAR,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
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
    name: String,
    columns: Vec<Column>,
}

impl Table {
    pub fn new(name: String, columns: &[(String, ColumnType)]) -> Result<Self, TableError> {
        if columns.len() == 0 {
            return Err(TableError::NoColumns(name))
        }

        let invalid_columns: Vec<String> = columns.iter()
            .map(|x| (&x.0, validate_name(&x.0)))
            .filter(|x| !x.1)
            .map(|x| x.0.clone())
            .collect();

        // TODO check if column names are unique
        if invalid_columns.len() > 0 {
            return Err(TableError::InvalidColumnName(name, invalid_columns))
        }

        if !validate_name(&name) {
            return Err(TableError::InvalidTableName(name.into()))
        }

        Ok(Table {
            name,
            columns: columns.iter().map(|c| Column {
                name: c.0.clone(),
                column_type: c.1,
                files: vec![],
            }).collect()
        })
    }

    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn new_filenames(&self) -> Vec<(String, ColumnType, String)> {
        self.columns.iter().map(|col| (
            col.name.clone(),
            col.column_type,
            generate_column_filename(
                &self.name,
                &col.name,
                col.files.len().try_into().unwrap(),
            ),
        )).collect()
    }

    pub fn add_filenames(&mut self, filenames: Vec<(String, ColumnType, String)>) -> Result<(), ()> {
        // TODO maybe could be enforced by the typesystem
        if filenames.len() != self.columns.len() {
            return Err(())
        }

        for (col, (name, typ, filename)) in self.columns.iter().zip(filenames.iter()) {
            if col.name != *name && col.column_type != *typ {
                return Err(())
            }
        }

        for (col, (name, typ, filename)) in self.columns.iter_mut().zip(filenames) {
            assert!(col.name == name && col.column_type == typ);
            col.files.push(filename);
        }

        Ok(())
    }
}

impl TryFrom<&[u8]> for Table {
    type Error = serde_json::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        serde_json::from_slice(value)
    }
}

impl TryInto<String> for &Table {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<String, Self::Error> {
        serde_json::to_string(&self)
    }
}

#[derive(Debug)]
pub enum TableError {
    InvalidTableName(String),
    InvalidColumnName(String, Vec<String>),
    DuplicatedColumns(String, Vec<String>),
    NoColumns(String),
}

pub fn create_schema_dir(data_path: &Path) -> std::io::Result<()> {
    create_dir_all(data_path.join(DATA_DIRNAME))?;
    create_dir_all(data_path.join(SCHEMA_DIRNAME))?;

    Ok(())
}
