pub fn data_key(s3_prefix: &str, path: &str) -> String {
    format!("{}data/{}", s3_prefix, path)
}

pub fn manifest_key(s3_prefix: &str) -> String {
    format!("{}manifest", s3_prefix)
}
