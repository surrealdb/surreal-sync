use std::collections::BTreeMap;
use std::fmt;

use crate::error::Error;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct Gtid {
    pub source_id: [u8; 16],
    pub gno: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct MySqlGtidSet {
    intervals: BTreeMap<[u8; 16], Vec<(u64, u64)>>,
}

// A derived serde impl would emit a JSON object keyed by the raw `[u8; 16]`
// source id, which serde_json rejects ("key must be a string"). Persist the set
// as its canonical `uuid:interval[,uuid:interval]` string instead, so GTID
// checkpoints round-trip through JSON stores (filesystem and SurrealDB).
impl serde::Serialize for MySqlGtidSet {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for MySqlGtidSet {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        MySqlGtidSet::parse(&s).map_err(serde::de::Error::custom)
    }
}

impl MySqlGtidSet {
    pub fn parse(input: &str) -> Result<Self, Error> {
        let mut set = Self::default();
        for part in input.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (uuid, range) = part
                .split_once(':')
                .ok_or_else(|| Error::GtidParse(format!("invalid gtid segment: {part}")))?;
            let source_id = parse_uuid(uuid)?;
            let (start, end) = parse_range(range)?;
            set.add_interval(source_id, start, end)?;
        }
        Ok(set)
    }

    pub fn add_interval(&mut self, source_id: [u8; 16], start: u64, end: u64) -> Result<(), Error> {
        if start == 0 || end < start {
            return Err(Error::GtidParse(format!("invalid interval {start}-{end}")));
        }
        let intervals = self.intervals.entry(source_id).or_default();
        intervals.push((start, end));
        intervals.sort_by_key(|(s, _)| *s);
        merge_intervals(intervals);
        Ok(())
    }

    pub fn add_gtid(&mut self, gtid: Gtid) -> Result<(), Error> {
        self.add_interval(gtid.source_id, gtid.gno, gtid.gno)
    }

    pub fn is_empty(&self) -> bool {
        self.intervals.is_empty()
    }

    /// Encode the set in MySQL's binary `Gtid_set` wire format, as consumed by
    /// `COM_BINLOG_DUMP_GTID`:
    ///
    /// ```text
    /// u64  n_sids
    /// per sid:
    ///   [u8; 16]  source uuid
    ///   u64       n_intervals
    ///   per interval:
    ///     u64  start        (inclusive)
    ///     u64  end          (EXCLUSIVE — last gno + 1)
    /// ```
    ///
    /// All integers are little-endian. Intervals are stored inclusively here, so
    /// the exclusive end is `end + 1`.
    pub fn to_binary(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.intervals.len() as u64).to_le_bytes());
        for (source_id, intervals) in &self.intervals {
            out.extend_from_slice(source_id);
            out.extend_from_slice(&(intervals.len() as u64).to_le_bytes());
            for (start, end) in intervals {
                out.extend_from_slice(&start.to_le_bytes());
                out.extend_from_slice(&(end + 1).to_le_bytes());
            }
        }
        out
    }
}

impl fmt::Display for MySqlGtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (source_id, intervals) in &self.intervals {
            for (start, end) in intervals {
                if !first {
                    write!(f, ", ")?;
                }
                first = false;
                write!(f, "{}:", format_uuid(source_id))?;
                if start == end {
                    write!(f, "{start}")?;
                } else {
                    write!(f, "{start}-{end}")?;
                }
            }
        }
        Ok(())
    }
}

fn merge_intervals(intervals: &mut Vec<(u64, u64)>) {
    if intervals.is_empty() {
        return;
    }
    let mut merged = vec![intervals[0]];
    for &(start, end) in intervals.iter().skip(1) {
        let last = merged.last_mut().unwrap();
        if start <= last.1 + 1 {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }
    *intervals = merged;
}

fn parse_uuid(s: &str) -> Result<[u8; 16], Error> {
    let hex = s.replace('-', "");
    if hex.len() != 32 {
        return Err(Error::GtidParse(format!("invalid uuid: {s}")));
    }
    let bytes = hex::decode(hex).map_err(|e| Error::GtidParse(e.to_string()))?;
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes);
    Ok(out)
}

fn format_uuid(id: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7], id[8], id[9], id[10], id[11],
        id[12], id[13], id[14], id[15]
    )
}

fn parse_range(s: &str) -> Result<(u64, u64), Error> {
    if let Some((start, end)) = s.split_once('-') {
        Ok((
            start.parse().map_err(|_| Error::GtidParse(s.into()))?,
            end.parse().map_err(|_| Error::GtidParse(s.into()))?,
        ))
    } else {
        let n: u64 = s.parse().map_err(|_| Error::GtidParse(s.into()))?;
        Ok((n, n))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_gtid_set() {
        let set = MySqlGtidSet::parse("d4c17f0c-8f94-11e9-8f94-000000000001:1-5").unwrap();
        assert_eq!(set.intervals.len(), 1);
    }

    #[test]
    fn binary_encoding_matches_mysql_layout() {
        let set = MySqlGtidSet::parse("00000000-0000-0000-0000-000000000001:1-5").unwrap();
        let bin = set.to_binary();
        let mut expected = Vec::new();
        expected.extend_from_slice(&1u64.to_le_bytes()); // n_sids
        let mut sid = [0u8; 16];
        sid[15] = 1;
        expected.extend_from_slice(&sid);
        expected.extend_from_slice(&1u64.to_le_bytes()); // n_intervals
        expected.extend_from_slice(&1u64.to_le_bytes()); // start (inclusive)
        expected.extend_from_slice(&6u64.to_le_bytes()); // end (exclusive = 5 + 1)
        assert_eq!(bin, expected);
    }

    #[test]
    fn serde_roundtrips_through_json_string() {
        let set = MySqlGtidSet::parse("00000000-0000-0000-0000-000000000001:1-5").unwrap();
        let json = serde_json::to_string(&set).unwrap();
        // Serialized as a JSON string, not an object keyed by raw bytes.
        assert!(json.starts_with('"'), "expected JSON string, got {json}");
        let back: MySqlGtidSet = serde_json::from_str(&json).unwrap();
        assert_eq!(back, set);
    }
}
