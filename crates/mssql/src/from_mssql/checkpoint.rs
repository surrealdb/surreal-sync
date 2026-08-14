//! SQL Server CDC log sequence number checkpoint.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 10-byte CDC LSN. Ordered by binary compare.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MssqlLsn(pub Vec<u8>);

impl MssqlLsn {
    /// CDC LSNs are 10 bytes.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() != 10 && !bytes.is_empty() {
            anyhow::bail!("SQL Server LSN must be 10 bytes, got {}", bytes.len());
        }
        Ok(Self(bytes))
    }

    /// Hex of the LSN bytes (no prefix).
    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }

    /// Parse hex (optional `0x` / `mssql:` prefix).
    pub fn from_hex(s: &str) -> Result<Self> {
        let s = s
            .strip_prefix("mssql:")
            .or_else(|| s.strip_prefix("0x"))
            .or_else(|| s.strip_prefix("0X"))
            .unwrap_or(s)
            .trim();
        let bytes = hex::decode(s).map_err(|e| anyhow!("invalid LSN hex `{s}`: {e}"))?;
        Self::from_bytes(bytes)
    }
}

impl std::fmt::Display for MssqlLsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Persisted SQL Server CDC checkpoint (LSN + timestamp).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MssqlCheckpoint {
    pub lsn: MssqlLsn,
    pub timestamp: DateTime<Utc>,
}

impl MssqlCheckpoint {
    /// Checkpoint at `lsn` with the current time.
    pub fn new(lsn: MssqlLsn) -> Self {
        Self {
            lsn,
            timestamp: Utc::now(),
        }
    }
}

impl surreal_sync_core::Checkpoint for MssqlCheckpoint {
    const DATABASE_TYPE: &'static str = "mssql";

    fn to_cli_string(&self) -> String {
        self.lsn.to_hex()
    }

    fn from_cli_string(s: &str) -> Result<Self> {
        let s = s.strip_prefix("mssql:").unwrap_or(s);
        Ok(Self {
            lsn: MssqlLsn::from_hex(s)?,
            timestamp: Utc::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use surreal_sync_core::Checkpoint;

    #[test]
    fn lsn_orders_binary() {
        let a = MssqlLsn::from_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 1]).unwrap();
        let b = MssqlLsn::from_bytes(vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 2]).unwrap();
        assert!(a < b);
    }

    #[test]
    fn checkpoint_cli_roundtrip() {
        let original =
            MssqlCheckpoint::new(MssqlLsn::from_bytes(vec![0, 0, 0, 0, 0, 0, 0, 1, 2, 3]).unwrap());
        let cli = original.to_cli_string();
        let decoded = MssqlCheckpoint::from_cli_string(&cli).unwrap();
        assert_eq!(decoded.lsn, original.lsn);
        let prefixed = MssqlCheckpoint::from_cli_string(&format!("mssql:{cli}")).unwrap();
        assert_eq!(prefixed.lsn, original.lsn);
    }
}
