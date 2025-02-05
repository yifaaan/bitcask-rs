use crate::data::log_record::LogRecordPos;

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to read from data file")]
    FailedToReadFromDataFile,

    #[error("Failed to write to data file")]
    FailedToWriteToDataFile,

    #[error("Failed to sync data file")]
    FailedToSyncDataFile,

    #[error("Key is empty")]
    KeyIsEmpty,

    #[error("Failed to open data file")]
    FailedToOpenDataFile,

    #[error("Failed to update index")]
    FailedToUpdateIndex,

    #[error("Key not found")]
    KeyNotFound,

    #[error("Data file not found")]
    DataFileNotFound,

    #[error("Invalid database directory")]
    InvalidDbDir,

    #[error("Invalid data file size")]
    InvalidDataFileSize,

    #[error("Failed to create database directory")]
    FailedToCreateDbDir,

    #[error("Failed to read directory")]
    FailedToReadDir,

    #[error("Failed to read directory entry")]
    FailedToReadDirEntry,

    #[error("Failed to parse file id")]
    FailedToParseFileId,

    #[error("Failed to create data file")]
    FailedToCreateDataFile,
}
