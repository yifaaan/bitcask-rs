use std::result;

use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
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

    #[error("key not found in database")]
    KeyNotFound,

    #[error("data file not found in database")]
    DataFileNotFound,

    #[error("database dir can not be empty")]
    DirPathIsEmpty,

    #[error("database data file size must be greater than 0")]
    DataFileSizeTooSmall,

    #[error("failed to create database dir")]
    FailedToCreateDatabaseDir,

    #[error("failed to read database dir")]
    FailedToReadDatabaseDir,

    #[error("the database directory maybe corrupted")]
    DataDirectoryCorrupted,

    #[error("read data file eof")]
    ReadDataFileEof,
}

pub type Result<T> = result::Result<T, Error>;
