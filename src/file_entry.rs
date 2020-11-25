use std::path::Path;
use std::io::SeekFrom;

use tokio::prelude::*;
use tokio::fs;

use super::error::Error;
use super::mmap;

#[derive(Debug, Clone)]
pub struct FileEntry {
    path: String,
    size: usize,
}

impl FileEntry {
    pub async unsafe fn open(&self) -> Result<mmap::Handle, Error> {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&self.path)
            .await?;
        let handle = mmap::Handle::new(file, self.size)?;
        Ok(handle)
    }

    pub async fn create(&self) -> Result<mmap::Handle, Error> {
        if let Some(dir) = Path::new(&self.path).parent() {
            fs::create_dir_all(dir).await?;
        }
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&self.path)
            .await?;
        if self.size > 0 {
            file.seek(SeekFrom::Start(self.size as u64 - 1)).await?;
            file.write(&[0]).await?;
        }
        let handle = unsafe { mmap::Handle::new(file, self.size) }?;
        Ok(handle)
    }

    pub fn new(path: String, size: usize) -> FileEntry {
        FileEntry { path, size }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn size(&self) -> usize {
        self.size
    }
}
