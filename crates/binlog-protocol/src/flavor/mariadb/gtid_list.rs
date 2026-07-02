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
        self.gtids.insert(gtid);
        Ok(())
    }
}

impl fmt::Display for MariaDbGtidList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, gtid) in self.gtids.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
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
}
