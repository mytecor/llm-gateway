#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid_rule: {0}")]
    InvalidRule(String),
    #[error("unsupported_language: {0}")]
    UnsupportedLanguage(String),
    #[error("parse_error: invalid UTF-8")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("parse_error: {0}")]
    Parse(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("resource limit exceeded: {0}")]
    Limit(&'static str),
    #[error("healing_failed: {0}")]
    Healing(String),
    #[error("rule revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict { expected: u64, actual: u64 },
    #[error("rule store lock poisoned")]
    StorePoisoned,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
pub type Result<T> = std::result::Result<T, Error>;
