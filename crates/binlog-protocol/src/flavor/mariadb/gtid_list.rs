use std::collections::BTreeSet;
use std::fmt;

use crate::error::Error;

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct MariaDbGtid {
    pub domain_id: u32,
    pub server_id: u32,
    pub sequence: u64,
}

#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct MariaDbGtidList {
    gtids: BTreeSet<MariaDbGtid>,
}

impl MariaDbGtidList {
    pub fn parse(input: &str) -> Result<Self, Error> {
        let mut list = Self::default();
        for part in input.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            list.add(parse_gtid(part)?)?;
        }
        Ok(list)
    }

    pub fn add(&mut self, gtid: MariaDbGtid) -> Result<(), Error> {
        // A MariaDB GTID position holds a single entry per replication domain —
        // the most advanced sequence. Replace any existing entry for this domain
        // when the incoming sequence is newer, so accumulated runtime positions
        // stay clean (and monotonic) rather than growing without bound.
        if let Some(existing) = self
            .gtids
            .iter()
            .find(|g| g.domain_id == gtid.domain_id)
            .cloned()
        {
            if existing.sequence >= gtid.sequence {
                return Ok(());
            }
            self.gtids.remove(&existing);
        }
        self.gtids.insert(gtid);
        Ok(())
    }

    /// Serialize as the compact comma-separated form MariaDB expects for
    /// `@slave_connect_state` / `gtid_slave_pos` (e.g. `0-1-270,1-7-42`).
    pub fn to_connect_state(&self) -> String {
        self.gtids
            .iter()
            .map(|g| g.to_string())
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl fmt::Display for MariaDbGtidList {
    /// Render the compact comma-separated form (no spaces) so `Display` output is
    /// identical to [`MariaDbGtidList::to_connect_state`] and re-parses via
    /// [`MariaDbGtidList::parse`] — CLI checkpoint strings must round-trip exactly.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, gtid) in self.gtids.iter().enumerate() {
            if idx > 0 {
                write!(f, ",")?;
            }
            write!(f, "{gtid}")?;
        }
        Ok(())
    }
}

impl fmt::Display for MariaDbGtid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", self.domain_id, self.server_id, self.sequence)
    }
}

fn parse_gtid(s: &str) -> Result<MariaDbGtid, Error> {
    let parts: Vec<_> = s.split('-').collect();
    if parts.len() != 3 {
        return Err(Error::GtidParse(format!("invalid mariadb gtid: {s}")));
    }
    Ok(MariaDbGtid {
        domain_id: parts[0].parse().map_err(|_| Error::GtidParse(s.into()))?,
        server_id: parts[1].parse().map_err(|_| Error::GtidParse(s.into()))?,
        sequence: parts[2].parse().map_err(|_| Error::GtidParse(s.into()))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_gtid_list() {
        let list = MariaDbGtidList::parse("0-1-270").unwrap();
        assert_eq!(list.gtids.len(), 1);
    }

    #[test]
    fn multi_domain_display_matches_connect_state_and_reparses() {
        let list = MariaDbGtidList::parse("0-1-270,1-7-42").unwrap();
        assert_eq!(list.gtids.len(), 2);
        // Display and to_connect_state must be identical (comma, no space) so CLI
        // strings are consistent and re-parseable.
        assert_eq!(list.to_string(), "0-1-270,1-7-42");
        assert_eq!(list.to_string(), list.to_connect_state());
        let reparsed = MariaDbGtidList::parse(&list.to_string()).unwrap();
        assert_eq!(reparsed, list);
    }

    #[test]
    fn parse_tolerates_spaces_after_comma() {
        // Older Display output used ", "; ensure such strings still parse.
        let list = MariaDbGtidList::parse("0-1-270, 1-7-42").unwrap();
        assert_eq!(list, MariaDbGtidList::parse("0-1-270,1-7-42").unwrap());
    }
}
