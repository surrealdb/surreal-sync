use surreal_sync::{SourceOpts, SurrealOpts};

#[test]
fn test_source_opts_creation() {
    let opts = SourceOpts {
        source_uri: "mongodb://test:test@localhost:27017".to_string(),
        source_database: Some("test_db".to_string()),
        source_username: Some("test".to_string()),
        source_password: Some("password".to_string()),
        neo4j_timezone: "UTC".to_string(),
    };

    assert_eq!(opts.source_uri, "mongodb://test:test@localhost:27017");
    assert_eq!(opts.source_database, Some("test_db".to_string()));
    assert_eq!(opts.source_username, Some("test".to_string()));
    assert_eq!(opts.source_password, Some("password".to_string()));
    assert_eq!(opts.neo4j_timezone, "UTC");
}

#[test]
fn test_surreal_opts_creation() {
    let opts = SurrealOpts {
        surreal_endpoint: "ws://localhost:8000".to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 1000,
        dry_run: false,
    };

    assert_eq!(opts.surreal_endpoint, "ws://localhost:8000");
    assert_eq!(opts.surreal_username, "root");
    assert_eq!(opts.surreal_password, "root");
    assert_eq!(opts.batch_size, 1000);
    assert!(!opts.dry_run);
}

#[test]
fn test_dry_run_flag() {
    let opts = SurrealOpts {
        surreal_endpoint: "ws://localhost:8000".to_string(),
        surreal_username: "root".to_string(),
        surreal_password: "root".to_string(),
        batch_size: 100,
        dry_run: true,
    };

    assert!(opts.dry_run);
}
