//! S3 semantics shared by every backend: the service applies these so backends stay dumb.

pub mod checksum;
pub mod etag;
pub mod listing;
pub mod validate;
