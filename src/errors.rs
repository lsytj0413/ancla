use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("data buffer is too small, expect {expect}, got {got}")]
    TooSmallData { expect: usize, got: usize },
}
