use crate::binlog_protocol::flavor::Flavor;

pub fn detect_flavor(version_string: &str) -> Flavor {
    let lower = version_string.to_ascii_lowercase();
    if lower.contains("mariadb") {
        Flavor::MariaDb
    } else {
        Flavor::MySql
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_mysql() {
        assert_eq!(detect_flavor("8.0.36"), Flavor::MySql);
    }

    #[test]
    fn detects_mariadb() {
        assert_eq!(detect_flavor("11.4.5-MariaDB-log"), Flavor::MariaDb);
    }
}
