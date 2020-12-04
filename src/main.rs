use tokio_compat::runtime;

use std::env;
use std::str::FromStr;

use rusoto_core::Region;

use clap::{App, Arg, ArgMatches, SubCommand};

mod chan_exec;
mod create;
mod error;
mod extract;
mod file_entry;
mod key_resolver;
mod mmap;
mod utils;

use error::Error;

fn args() -> ArgMatches<'static> {
    App::new("s3ar")
        .about("Massively fast S3 downloader/uploader")
        .arg(
            Arg::with_name("directory")
                .short("C")
                .long("directory")
                .value_name("DIR")
                .help("Sets the current directory")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("upload")
                .arg(
                    Arg::with_name("file_concurrency")
                        .short("F")
                        .long("file-concurrency")
                        .value_name("NUM")
                        .help("Sets the concurrency of files")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("part_concurrency")
                        .short("P")
                        .long("part-concurrency")
                        .value_name("NUM")
                        .help("Sets the concurrency of parts")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("part_queue_size")
                        .short("Q")
                        .long("part-queue-size")
                        .value_name("NUM")
                        .help("Sets the size of parts queue")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("part_size")
                        .short("s")
                        .long("part-size")
                        .value_name("SIZE")
                        .help("Sets the part size in bytes")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("TARGET_BUCKET")
                        .help("Sets the S3 bucket")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("TARGET_PREFIX")
                        .help("Sets the S3 prefix")
                        .required(true)
                        .index(2),
                )
                .arg(
                    Arg::with_name("FILE")
                        .help("Sets the files to archive")
                        .required(true)
                        .index(3)
                        .multiple(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("download")
                .arg(
                    Arg::with_name("file_concurrency")
                        .short("F")
                        .long("file-concurrency")
                        .value_name("NUM")
                        .help("Sets the concurrency of files")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("part_concurrency")
                        .short("P")
                        .long("part-concurrency")
                        .value_name("NUM")
                        .help("Sets the concurrency of parts")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("SOURCE_BUCKET")
                        .help("Sets the S3 bucket")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("SOURCE_PREFIX")
                        .help("Sets the S3 prefix")
                        .required(true)
                        .index(2),
                )
        )
        .get_matches()
}

fn main() {
    let aws_region = if let Ok(endpoint) = env::var("S3_ENDPOINT") {
        let region = Region::Custom { name: "ap-northeast-1".to_owned(), endpoint: endpoint.to_owned() };
        println!("picked up non-standard endpoint {:?} from S3_ENDPOINT env. variable", region);
        region
    } else {
        Region::ApNortheast1
    };

    let matches = args();

    let mut rt = runtime::Builder::default()
        .core_threads(4)
        .build()
        .expect("failed to create Runtime");

    let s3_client = rusoto_s3::S3Client::new(aws_region);
    if let Some(sub_matches) = matches.subcommand_matches("upload") {
        let creator = create::CreateExecutor::new(s3_client);
        let fut = creator.execute(build_archive_create(&matches, &sub_matches));
        rt.block_on_std(fut).expect("failed to execute");
        return;
    }
    if let Some(sub_matches) = matches.subcommand_matches("download") {
        let extractor = extract::ExtractExecutor::new(s3_client);
        let fut = extractor.execute(build_archive_extract(&matches, &sub_matches));
        rt.block_on_std(fut).expect("failed to execute");
        return;
    }
}

fn build_archive_create(matches: &ArgMatches, sub_matches: &ArgMatches) -> create::ArchiveCreate {
    let directory = matches.value_of_os("directory").map(Into::into);

    let files = sub_matches
        .values_of("FILE")
        .expect("no files to archive")
        .map(Into::into)
        .collect();

    let file_concurrency = sub_matches
        .value_of("file_concurrency")
        .map(FromStr::from_str)
        .unwrap_or(Ok(8))
        .expect("failed to parse file concurrency");
    let part_concurrency = sub_matches
        .value_of("part_concurrency")
        .map(FromStr::from_str)
        .unwrap_or(Ok(8))
        .expect("failed to parse part concurrency");
    let part_queue_size = sub_matches
        .value_of("part_queue_size")
        .map(FromStr::from_str)
        .unwrap_or(Ok(8))
        .expect("failed to parse part queue size");
    let part_size = sub_matches
        .value_of("part_size")
        .map(FromStr::from_str)
        .unwrap_or(Ok(16usize * 1024 * 1024))
        .expect("failed to parse part size");

    let s3_bucket = sub_matches
        .value_of("TARGET_BUCKET")
        .expect("no s3 bucket")
        .to_string();
    let s3_prefix = sub_matches
        .value_of("TARGET_PREFIX")
        .expect("no s3 prefix")
        .to_string();

    create::ArchiveCreate {
        file_concurrency,
        part_concurrency,
        part_queue_size,
        part_size,
        s3_bucket,
        s3_prefix,
        directory,
        files,
    }
}


fn build_archive_extract(matches: &ArgMatches, sub_matches: &ArgMatches) -> extract::ArchiveExtract {
    let directory = matches.value_of_os("directory").map(Into::into);

    let file_concurrency = sub_matches
        .value_of("file_concurrency")
        .map(FromStr::from_str)
        .unwrap_or(Ok(8))
        .expect("failed to parse file concurrency");
    let part_concurrency = sub_matches
        .value_of("part_concurrency")
        .map(FromStr::from_str)
        .unwrap_or(Ok(8))
        .expect("failed to parse part concurrency");

    let s3_bucket = sub_matches
        .value_of("SOURCE_BUCKET")
        .expect("no s3 bucket")
        .to_string();
    let s3_prefix = sub_matches
        .value_of("SOURCE_PREFIX")
        .expect("no s3 prefix")
        .to_string();

    extract::ArchiveExtract {
        file_concurrency,
        part_concurrency,
        s3_bucket,
        s3_prefix,
        directory,
    }
}
