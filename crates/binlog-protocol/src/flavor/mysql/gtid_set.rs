use std::collections::BTreeMap;
use std::fmt;

use crate::error::Error;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct Gtid {
    pub source_id: [u8; 16],
    pub gno: u64,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct MySqlGtidSet {
    intervals: BTreeMap<[u8; 16], Vec<(u64, u64)>>,
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
}
