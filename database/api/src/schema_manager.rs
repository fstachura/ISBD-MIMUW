use std::{fs::File, io::{Read, SeekFrom, Seek, Write}, path::PathBuf};
use schema::{Schema, get_schema_path};

pub struct SchemaManager {
    data_dir: PathBuf,
}

impl SchemaManager {
    pub fn new(data_dir: PathBuf) -> Self {
        SchemaManager { data_dir }
    }
}

pub struct RoSchema {
    schema: Schema,
    // shared file lock - enforced by get_schema
    #[allow(unused)]
    file: File,
}

impl RoSchema {
    pub fn get(&self) -> &Schema {
        &self.schema
    }
}

pub struct MutSchema {
    schema: Schema,
    // exclusive file lock - enforced by get_schema
    file: File,
}

impl MutSchema {
    pub fn get(&self) -> &Schema {
        &self.schema
    }

    pub fn get_mut(&mut self) -> &mut Schema {
        &mut self.schema
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        let data: String = (&self.schema).try_into()?;
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.write(&data.as_bytes())?;
        self.file.sync_all()?;
        Ok(())
    }
}

impl SchemaManager {
    pub async fn get_schema(&self) -> std::io::Result<RoSchema> {
        let data_dir = self.data_dir.clone();
        tokio::task::spawn_blocking(move || {
            let mut file = File::options()
                .read(true)
                .open(get_schema_path(&data_dir))?;

            file.lock_shared()?;

            let mut data = vec![];
            file.read_to_end(&mut data)?;
            Ok(RoSchema {
                schema: Schema::try_from(data.as_slice())?,
                file,
            })
        }).await?
    }

    pub async fn get_mut_schema(&self) -> std::io::Result<MutSchema> {
        let data_dir = self.data_dir.clone();
        tokio::task::spawn_blocking(move || {
            let mut file = File::options()
                .read(true)
                .write(true)
                .open(get_schema_path(&data_dir))?;

            file.lock()?;

            let mut data = vec![];
            file.read_to_end(&mut data)?;
            Ok(MutSchema {
                schema: Schema::try_from(data.as_slice())?,
                file,
            })
        }).await?
    }
}
