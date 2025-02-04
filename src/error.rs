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
}
