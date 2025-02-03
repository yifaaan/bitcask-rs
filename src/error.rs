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

    #[error("Failed to open data file")]
    FailedToOpenDataFile,
}
