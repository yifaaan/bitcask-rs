use std::result;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("failed to read from data file")]
    FailedToReadFromDataFile,

    #[error("failed to write to data file")]
    FailedToWriteToDataFile,

    #[error("failed to sync data file")]
    FailedToSyncDataFile,

    #[error("failed to open data file")]
    FailedToOpenDataFile,

    #[error("key is empty")]
    KeyIsEmpty,

    #[error("index update failed")]
    IndexUpdateFail,
}

pub type Result<T> = result::Result<T, Error>;
