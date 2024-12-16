use std::result;

pub enum Error {}

pub type Result<T> = result::Result<T, Error>;
