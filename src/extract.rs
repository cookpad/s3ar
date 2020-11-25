use std::path::PathBuf;

use futures::compat::*;
use futures::prelude::*;

use rusoto_s3::{GetObjectOutput, GetObjectRequest, S3Client, S3};

use super::file_entry::FileEntry;
use super::key_resolver;
use super::mmap;
use super::utils::with_retry;
use super::Error;

#[derive(Debug, Clone)]
pub struct ArchiveExtract {
    pub file_concurrency: usize,
    pub part_concurrency: usize,
    pub directory: Option<PathBuf>,
    pub s3_bucket: String,
    pub s3_prefix: String,
}

pub struct ExtractExecutor {
    s3_client: S3Client,
}

impl ExtractExecutor {
    pub fn new(s3_client: S3Client) -> Self {
        Self { s3_client }
    }

    pub async fn execute(
        &self,
        ArchiveExtract {
            file_concurrency,
            part_concurrency,
            directory,
            s3_bucket,
            s3_prefix,
        }: ArchiveExtract,
    ) -> Result<(), Error> {
        if let Some(cwd) = directory {
            std::env::set_current_dir(cwd).expect("failed to change current dir");
        }

        let get_object_request = GetObjectRequest {
            bucket: s3_bucket.clone(),
            key: key_resolver::manifest_key(&s3_prefix),
            ..Default::default()
        };
        let GetObjectOutput { body, .. } = self
            .s3_client
            .get_object(get_object_request)
            .compat()
            .await?;

        let mp_downloader = MultipartDownloadExecutor {
            s3_client: self.s3_client.clone(),
        };
        let mp_downloader = &mp_downloader;

        body.expect("no manifest content")
            .compat()
            .into_async_read()
            .lines()
            .map_err(Error::from)
            .and_then(|line| {
                async move {
                    let mut cols = line.split("\t");
                    let size = cols.next().unwrap().parse().map_err(|e| format!("{}", e))?;
                    let path = cols.next().ok_or("no path in manifest")?;
                    Ok(FileEntry::new(path.to_string(), size))
                }
            })
            .map_ok(|entry| {
                let s3_prefix = s3_prefix.clone();
                let s3_bucket = s3_bucket.clone();

                let source_key = key_resolver::data_key(&s3_prefix, entry.path());
                let object_download = ObjectDownload {
                    source_bucket: s3_bucket,
                    source_key,
                };
                with_retry(10, 1, 5, move || {
                    mp_downloader.execute(object_download.clone(), entry.clone())
                })
            })
            .try_buffer_unordered(file_concurrency)
            .try_flatten()
            .try_for_each_concurrent(part_concurrency, |fut| {
                async move {
                    fut.await
                }
            })
            .await
    }
}

#[derive(Debug, Clone)]
pub struct ObjectDownload {
    source_bucket: String,
    source_key: String,
}

pub struct MultipartDownloadExecutor {
    s3_client: S3Client,
}

impl MultipartDownloadExecutor {
    async fn execute(
        &self,
        ObjectDownload {
            source_bucket,
            source_key,
        }: ObjectDownload,
        target: FileEntry,
    ) -> Result<impl Stream<Item = Result<impl Future<Output = Result<(), Error>>, Error>>, Error> {
        let handle = target.create().await?;
        let chunker = mmap::Chunker::new(handle);
        let s3 = self.s3_client.clone();
        Ok(stream::try_unfold((chunker, None), move |(mut chunker, state)| {
            let s3 = s3.clone();
            let bucket = source_bucket.clone();
            let key = source_key.clone();
            async move {
                let (part, parts_count, next_part_number) =
                    if let Some((part_number, parts_count)) = state {
                        if part_number > parts_count {
                            return Ok(None);
                        }
                        let part = get_part(&s3, bucket.clone(), key.clone(), part_number).await?;
                        (part, parts_count, part_number + 1)
                    } else {
                        let part = get_part(&s3, bucket.clone(), key.clone(), 1).await?;
                        let parts_count = part.parts_count.ok_or("no parts count header")?;
                        (part, parts_count, 2)
                    };
                let content_length = part.content_length.ok_or("no content length header")?;
                let chunk = chunker.take_chunk(content_length as usize);
                return Ok::<_, Error>(Some((
                    (part, chunk),
                    (chunker, Some((next_part_number, parts_count))),
                )));
            }
        })
        .map_ok(|(source, mut target)| {
            async move {
                let source_read =
                    source.body.ok_or("no body")?.compat().into_async_read();
                let mut target_write = futures::io::Cursor::new(&mut target[..]);
                futures::io::copy(source_read, &mut target_write).await?;
                Ok(())
            }
        }))
    }
}

async fn get_part(
    s3: &S3Client,
    bucket: String,
    key: String,
    part_number: i64,
) -> Result<GetObjectOutput, Error> {
    let request = GetObjectRequest {
        bucket,
        key,
        part_number: Some(part_number),
        ..Default::default()
    };
    let output = s3.get_object(request).compat().await?;
    Ok(output)
}
