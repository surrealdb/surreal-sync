use crate::flavor::Flavor;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("unsupported event type {event_type} for flavor {flavor:?}")]
    UnsupportedEvent { flavor: Flavor, event_type: u8 },

    #[error("gtid parse error: {0}")]
    GtidParse(String),

    #[error("incompatible resume position: expected {expected:?}, got {got:?}")]
    IncompatibleResume { expected: Flavor, got: String },

    #[error("encrypted binlog is not supported")]
    EncryptedBinlog,

    #[error("ssl is not supported in this version")]
    SslNotSupported,

    #[error("authentication error: {0}")]
    Auth(String),

    #[error("cell decode error: {0}")]
    CellDecode(String),

    #[error("unexpected end of buffer")]
    UnexpectedEof,
}
