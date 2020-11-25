use std::cmp;
use std::path::PathBuf;

use future::Either as E;
use futures::compat::*;
use futures::prelude::*;
use tokio::fs;
use tokio::prelude::*;

use rusoto_core::RusotoError;
use rusoto_s3::{
    CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart,
    CreateMultipartUploadOutput, CreateMultipartUploadRequest, PutObjectRequest, S3Client,
    UploadPartError, UploadPartOutput, UploadPartRequest, S3,
};

use super::chan_exec;
use super::file_entry::FileEntry;
use super::key_resolver;
use super::mmap;
use super::utils::with_retry;
use super::Error;

pub type PartUploadExecutor =
    chan_exec::ChanExec<Result<UploadPartOutput, RusotoError<UploadPartError>>>;

#[derive(Debug, Clone)]
pub struct ArchiveCreate {
    pub file_concurrency: usize,
    pub part_concurrency: usize,
    pub part_size: usize,
    pub part_queue_size: usize,
    pub directory: Option<PathBuf>,
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub files: Vec<PathBuf>,
}

pub struct CreateExecutor {
    s3_client: S3Client,
}

impl CreateExecutor {
    pub fn new(s3_client: S3Client) -> Self {
        Self { s3_client }
    }

    pub async fn execute(
        &self,
        ArchiveCreate {
            file_concurrency,
            part_concurrency,
            part_size,
            part_queue_size,
            directory,
            s3_bucket,
            s3_prefix,
            files,
        }: ArchiveCreate,
    ) -> Result<(), Error> {
        let (part_uploader, part_upload_tasks) = chan_exec::create(part_queue_size);
        let mp_uploader = MultipartUploadExecutor {
            s3_client: self.s3_client.clone(),
            part_uploader,
        };
        let main = MainExecutor {
            s3_client: self.s3_client.clone(),
            mp_uploader,
            file_concurrency,
            part_size,
        };
        let main_fut = async move {
            // Move main into async block and drop it after await
            // because the future won't get completed
            // if the ChanExec which main holds were not dropeed
            main.execute(s3_bucket, s3_prefix, directory, files).await
        };
        let part_upload_fut = part_upload_tasks
            .map(Ok)
            .try_for_each_concurrent(part_concurrency, |fut| {
                fut.map_err(|e| format!("{:?}", e).into())
            });
        future::try_join(main_fut, part_upload_fut)
            .await
            .map(|_| ())
    }
}

pub struct MainExecutor {
    s3_client: S3Client,
    mp_uploader: MultipartUploadExecutor,
    file_concurrency: usize,
    part_size: usize,
}

impl MainExecutor {
    pub async fn execute(
        &self,
        s3_bucket: String,
        s3_prefix: String,
        directory: Option<PathBuf>,
        files: Vec<PathBuf>,
    ) -> Result<(), Error> {
        if let Some(cwd) = directory {
            std::env::set_current_dir(cwd).expect("failed to change current dir");
        }

        let manifest = stream::iter(files)
            .map(read_dir_recur)
            .flatten()
            .map_err(Error::from)
            .map_ok(|entry| {
                async {
                    let object_upload = ObjectUpload {
                        target_bucket: s3_bucket.clone(),
                        target_key: key_resolver::data_key(&s3_prefix, entry.path()),
                    };
                    self.mp_uploader
                        .execute(self.part_size, object_upload, entry.clone())
                        .await?;
                    Ok(entry)
                }
            })
            .try_buffer_unordered(self.file_concurrency)
            .try_fold(Vec::<u8>::new(), |mut manifest, entry| {
                manifest.extend_from_slice(format!("{}\t", entry.size()).as_bytes());
                manifest.extend_from_slice(entry.path().as_bytes());
                manifest.push(b'\n');
                async move { Ok(manifest) }
            })
            .await?;

        let put_object_request = PutObjectRequest {
            bucket: s3_bucket.clone(),
            key: key_resolver::manifest_key(&s3_prefix),
            body: Some(manifest.into()),
            ..Default::default()
        };
        self.s3_client
            .put_object(put_object_request)
            .compat()
            .await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct MultipartUploadExecutor {
    s3_client: S3Client,
    part_uploader: PartUploadExecutor,
}

impl MultipartUploadExecutor {
    async fn execute(
        &self,
        part_size: usize,
        object_upload: ObjectUpload,
        source: FileEntry,
    ) -> Result<(), Error> {
        let body = unsafe { source.open() }.await?;
        let mp_start = MultipartUploadStart::new(object_upload);
        let CreateMultipartUploadOutput { upload_id, .. } = with_retry(10, 1, 5, || {
            self.s3_client
                .create_multipart_upload(mp_start.start())
                .compat()
        })
        .await?;
        let mp = mp_start.started(upload_id.expect("no upload_id in response"));
        let part_bodies = MultipartUpload::parts(part_size, body);
        let part_bodies_with_number = part_bodies.enumerate().map(|(i, b)| (i as i64 + 1, b));
        let mut completed_parts: Vec<_> = stream::iter(part_bodies_with_number)
            .map(Ok::<_, Error>)
            .map_ok(|(part_number, part_body)| {
                let mut exec = self.part_uploader.clone();
                let s3_client = self.s3_client.clone();
                let mp = mp.clone();
                async move {
                    let UploadPartOutput { e_tag, .. } = exec
                        .execute(
                            with_retry(10, 1, 5, move || {
                                let req = mp.upload_part(part_number, &part_body);
                                s3_client.upload_part(req).compat()
                            })
                            .boxed(),
                        )
                        .await??;
                    let part_number = Some(part_number);
                    Ok(CompletedPart { e_tag, part_number })
                }
            })
            .try_buffer_unordered(8)
            .try_collect()
            .await?;

        completed_parts.sort_by_key(|part| part.part_number);

        with_retry(10, 1, 5, move || {
            self.s3_client
                .complete_multipart_upload(mp.complete((&completed_parts).clone()))
                .compat()
        })
        .await?;
        Ok(())
    }
}

pub fn read_dir_recur(dir: PathBuf) -> stream::BoxStream<'static, io::Result<FileEntry>> {
    fs::read_dir(dir)
        .try_flatten_stream()
        .and_then(|entry| {
            async move {
                let metadata = entry.metadata().await?;
                let path = entry.path();
                if metadata.is_dir() {
                    return Ok(E::Left(E::Left(read_dir_recur(path))));
                } else if metadata.is_file() {
                    let size = metadata.len() as usize;
                    return Ok(E::Left(E::Right(stream::once(async move {
                        // do panic simply if file path contains non-UTF-8 strings
                        // because it's very rare and it doesn't have to be care
                        let path_string = path
                            .to_str()
                            .expect("non-UTF-8 strings in path")
                            .to_string();
                        Ok(FileEntry::new(path_string, size))
                    }))));
                }
                Ok(E::Right(stream::empty()))
            }
        })
        .try_flatten()
        .boxed()
}

#[derive(Debug, Clone)]
pub struct ObjectUpload {
    target_bucket: String,
    target_key: String,
}

pub struct MultipartUploadStart {
    obj: ObjectUpload,
}

impl MultipartUploadStart {
    pub fn new(obj: ObjectUpload) -> MultipartUploadStart {
        MultipartUploadStart { obj }
    }

    pub fn start(&self) -> CreateMultipartUploadRequest {
        CreateMultipartUploadRequest {
            bucket: self.obj.target_bucket.clone(),
            key: self.obj.target_key.clone(),
            ..Default::default()
        }
    }

    pub fn started(self, upload_id: String) -> MultipartUpload {
        MultipartUpload {
            obj: self.obj,
            upload_id,
        }
    }
}

#[derive(Clone)]
pub struct MultipartUpload {
    obj: ObjectUpload,
    upload_id: String,
}

impl MultipartUpload {
    pub fn parts(part_size: usize, mmap_handle: mmap::Handle) -> PartUploadBodies {
        let mmap_chunker = Some(mmap::Chunker::new(mmap_handle));
        PartUploadBodies {
            part_size,
            mmap_chunker,
        }
    }

    pub fn upload_part(&self, part_number: i64, body: &mmap::Chunk) -> UploadPartRequest {
        UploadPartRequest {
            body: Some(body.to_vec().into()),
            bucket: self.obj.target_bucket.clone(),
            key: self.obj.target_key.clone(),
            content_length: Some(body.len() as i64),
            part_number,
            upload_id: self.upload_id.clone(),
            ..Default::default()
        }
    }

    pub fn complete(&self, parts: Vec<CompletedPart>) -> CompleteMultipartUploadRequest {
        CompleteMultipartUploadRequest {
            bucket: self.obj.target_bucket.clone(),
            key: self.obj.target_key.clone(),
            multipart_upload: Some(CompletedMultipartUpload { parts: Some(parts) }),
            upload_id: self.upload_id.clone(),
            ..Default::default()
        }
    }
}

pub struct PartUploadBodies {
    part_size: usize,
    mmap_chunker: Option<mmap::Chunker>,
}

impl Iterator for PartUploadBodies {
    type Item = mmap::Chunk;
    fn next(&mut self) -> Option<Self::Item> {
        let mmap_chunker = match &mut self.mmap_chunker {
            Some(ref mut c) => c,
            None => {
                return None;
            }
        };
        let len = cmp::min(self.part_size, mmap_chunker.size());
        let mmap_chunk = mmap_chunker.take_chunk(len);
        if mmap_chunker.size() == 0 {
            self.mmap_chunker = None;
        }
        Some(mmap_chunk)
    }
}
