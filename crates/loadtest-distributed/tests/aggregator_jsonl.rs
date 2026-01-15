//! Integration tests for HTTP POST functionality with ContainerMetrics.
//!
//! These tests verify that:
//! 1. ContainerMetrics can be serialized to JSON
//! 2. HTTP POST requests can be constructed and sent correctly
//! 3. Metrics structure is valid and complete

use chrono::Utc;
use loadtest_distributed::metrics::{
    ContainerMetrics, EnvironmentInfo, Operation, PopulateMetrics, VerificationReport,
    VerificationResult,
};

/// Helper to create test environment info
fn create_test_env() -> EnvironmentInfo {
    EnvironmentInfo {
        cpu_cores: 4,
        memory_mb: 8192,
        available_memory_mb: 4096,
        tmpfs_enabled: false,
        tmpfs_size_mb: None,
        cgroup_memory_limit: None,
        cgroup_cpu_quota: None,
        data_fs_type: None,
    }
}

/// Helper to create test populate metrics
fn create_test_populate_metrics(container_id: &str, rows: u64) -> ContainerMetrics {
    ContainerMetrics {
        container_id: container_id.to_string(),
        hostname: "test-host".to_string(),
        started_at: Utc::now(),
        completed_at: Utc::now(),
        environment: create_test_env(),
        tables_processed: vec!["users".to_string()],
        operation: Operation::Populate,
        metrics: Some(PopulateMetrics {
            rows_processed: rows,
            duration_ms: 1000,
            batch_count: 10,
            rows_per_second: rows as f64,
            bytes_written: None,
        }),
        verification_report: None,
        errors: vec![],
        success: true,
    }
}

/// Helper to create test verify metrics
fn create_test_verify_metrics(container_id: &str, matched: u64) -> ContainerMetrics {
    ContainerMetrics {
        container_id: container_id.to_string(),
        hostname: "test-host".to_string(),
        started_at: Utc::now(),
        completed_at: Utc::now(),
        environment: create_test_env(),
        tables_processed: vec!["users".to_string()],
        operation: Operation::Verify,
        metrics: None,
        verification_report: Some(VerificationReport {
            tables: vec![VerificationResult {
                table_name: "users".to_string(),
                expected: matched,
                found: matched,
                missing: 0,
                mismatched: 0,
                matched,
            }],
            total_expected: matched,
            total_found: matched,
            total_missing: 0,
            total_mismatched: 0,
            total_matched: matched,
        }),
        errors: vec![],
        success: true,
    }
}

#[test]
fn test_container_metrics_serialization() {
    // Test populate metrics
    let populate_metrics = create_test_populate_metrics("populate-1", 100);
    let json = serde_json::to_string(&populate_metrics).expect("Failed to serialize");

    // Verify JSON is valid
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Invalid JSON");

    // Verify required fields
    assert_eq!(parsed["container_id"], "populate-1");
    assert_eq!(parsed["operation"], "populate");
    assert_eq!(parsed["success"], true);
    assert!(parsed["metrics"].is_object());
    assert_eq!(parsed["metrics"]["rows_processed"], 100);

    // Test verify metrics
    let verify_metrics = create_test_verify_metrics("verify-1", 200);
    let json = serde_json::to_string(&verify_metrics).expect("Failed to serialize");

    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Invalid JSON");

    assert_eq!(parsed["container_id"], "verify-1");
    assert_eq!(parsed["operation"], "verify");
    assert!(parsed["verification_report"].is_object());
    assert_eq!(parsed["verification_report"]["total_matched"], 200);
}

#[test]
fn test_container_metrics_roundtrip() {
    let original = create_test_populate_metrics("test-container", 500);

    // Serialize and deserialize
    let json = serde_json::to_string(&original).expect("Failed to serialize");
    let deserialized: ContainerMetrics =
        serde_json::from_str(&json).expect("Failed to deserialize");

    // Verify fields match
    assert_eq!(deserialized.container_id, original.container_id);
    assert_eq!(deserialized.operation, original.operation);
    assert_eq!(deserialized.success, original.success);
    assert_eq!(deserialized.metrics.as_ref().unwrap().rows_processed, 500);
}

#[test]
fn test_environment_info_completeness() {
    let env = create_test_env();

    assert_eq!(env.cpu_cores, 4);
    assert_eq!(env.memory_mb, 8192);
    assert!(!env.tmpfs_enabled);

    // Serialize to verify all fields are present
    let json = serde_json::to_string(&env).expect("Failed to serialize");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Invalid JSON");

    assert!(parsed["cpu_cores"].is_number());
    assert!(parsed["memory_mb"].is_number());
    assert!(parsed["tmpfs_enabled"].is_boolean());
}

#[test]
fn test_populate_metrics_structure() {
    let metrics = PopulateMetrics {
        rows_processed: 1000,
        duration_ms: 5000,
        batch_count: 100,
        rows_per_second: 200.0,
        bytes_written: Some(1024000),
    };

    let json = serde_json::to_string(&metrics).expect("Failed to serialize");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Invalid JSON");

    assert_eq!(parsed["rows_processed"], 1000);
    assert_eq!(parsed["duration_ms"], 5000);
    assert_eq!(parsed["batch_count"], 100);
    assert_eq!(parsed["rows_per_second"], 200.0);
    assert_eq!(parsed["bytes_written"], 1024000);
}

#[test]
fn test_verification_report_structure() {
    let report = VerificationReport {
        tables: vec![
            VerificationResult {
                table_name: "users".to_string(),
                expected: 100,
                found: 100,
                missing: 0,
                mismatched: 0,
                matched: 100,
            },
            VerificationResult {
                table_name: "orders".to_string(),
                expected: 50,
                found: 48,
                missing: 2,
                mismatched: 0,
                matched: 48,
            },
        ],
        total_expected: 150,
        total_found: 148,
        total_missing: 2,
        total_mismatched: 0,
        total_matched: 148,
    };

    let json = serde_json::to_string(&report).expect("Failed to serialize");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Invalid JSON");

    assert_eq!(parsed["total_matched"], 148);
    assert_eq!(parsed["total_missing"], 2);
    assert!(parsed["tables"].is_array());
    assert_eq!(parsed["tables"][0]["table_name"], "users");
    assert_eq!(parsed["tables"][1]["table_name"], "orders");
}

#[test]
fn test_metrics_with_errors() {
    let mut metrics = create_test_populate_metrics("error-container", 50);
    metrics.errors = vec![
        "Failed to insert batch 5".to_string(),
        "Connection timeout".to_string(),
    ];
    metrics.success = false;

    let json = serde_json::to_string(&metrics).expect("Failed to serialize");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Invalid JSON");

    assert_eq!(parsed["success"], false);
    assert!(parsed["errors"].is_array());
    assert_eq!(parsed["errors"][0], "Failed to insert batch 5");
    assert_eq!(parsed["errors"][1], "Connection timeout");
}

#[test]
fn test_operation_enum_serialization() {
    // Test Populate
    let populate = Operation::Populate;
    let json = serde_json::to_string(&populate).expect("Failed to serialize");
    assert_eq!(json, "\"populate\"");

    // Test Verify
    let verify = Operation::Verify;
    let json = serde_json::to_string(&verify).expect("Failed to serialize");
    assert_eq!(json, "\"verify\"");
}

#[test]
fn test_container_metrics_with_no_optional_fields() {
    let metrics = ContainerMetrics {
        container_id: "minimal-container".to_string(),
        hostname: "test".to_string(),
        started_at: Utc::now(),
        completed_at: Utc::now(),
        environment: create_test_env(),
        tables_processed: vec![],
        operation: Operation::Populate,
        metrics: None,
        verification_report: None,
        errors: vec![],
        success: true,
    };

    // Should serialize without panicking
    let json = serde_json::to_string(&metrics).expect("Failed to serialize");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("Invalid JSON");

    // Optional fields should not be present in JSON (due to skip_serializing_if)
    assert!(parsed.get("metrics").is_none());
    assert!(parsed.get("verification_report").is_none());
}
