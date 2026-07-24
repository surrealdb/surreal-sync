//! Replay captured binlog wire fixtures without Docker.

mod captured;

use captured::support::{capture_sql_block, flavor_from_meta, load_fixture, normalize_events};
use surreal_sync_mysql::binlog_protocol::{BinlogBytesReader, Flavor};

fn replay_fixture(base: &str, table: &str) {
    let (bytes, meta) = load_fixture(base);
    let flavor = flavor_from_meta(&meta);

    let mut reader = BinlogBytesReader::new(bytes, flavor);
    if meta.checksum_enabled {
        reader.enable_checksum();
    }
    let events = reader
        .all_events()
        .unwrap_or_else(|e| panic!("decode {base}: {e}"));
    assert!(
        reader.remaining() == 0,
        "{base}: trailing bytes remain after decode"
    );

    let snapshot = normalize_events(events, table);
    // Sort map keys so the snapshot is stable regardless of whether any crate in
    // the build graph enables `serde_json/preserve_order` (feature unification
    // otherwise flips struct/map key ordering between single-crate and
    // workspace-wide test runs).
    insta::with_settings!({sort_maps => true}, {
        insta::assert_json_snapshot!(format!("{base}_events"), snapshot);
    });
}

#[test]
fn replay_mysql_8_basic_fixture() {
    replay_fixture("mysql_8_basic", "wire_fixture");
}

#[test]
fn replay_mariadb_11_4_basic_fixture() {
    replay_fixture("mariadb_11_4_basic", "wire_fixture");
}

#[test]
fn captured_fixtures_exist() {
    for base in ["mysql_8_basic", "mariadb_11_4_basic"] {
        let (bin_path, meta_path) = captured::support::fixture_paths(base);
        assert!(
            bin_path.exists(),
            "missing bin fixture at {} — run CAPTURE_BINLOG_FIXTURES=1 cargo test -p surreal-sync-mysql capture",
            bin_path.display()
        );
        assert!(
            meta_path.exists(),
            "missing meta fixture at {}",
            meta_path.display()
        );
    }
    let _ = captured::support::FIXTURE_DIR;
}

#[test]
fn capture_sql_is_documented_for_each_flavor() {
    let mysql = capture_sql_block(Flavor::MySql);
    let mariadb = capture_sql_block(Flavor::MariaDb);
    assert!(mysql.contains("JSON NOT NULL"));
    assert!(mariadb.contains("LONGTEXT NOT NULL"));
    assert!(mariadb.contains("START TRANSACTION"));
}
