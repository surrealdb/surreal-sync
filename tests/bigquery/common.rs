//! Shared helpers for the BigQuery emulator integration tests.
//!
//! Every test in this directory runs against the shared emulator container from
//! [`shared_bigquery`] and the shared SurrealDB container, isolating itself with a
//! uniquely-named source table and a unique SurrealDB namespace/database.

#![allow(dead_code)]

use surreal_sync::testing::shared_containers::{
    shared_bigquery, shared_surrealdb, BIGQUERY_TEST_DATASET, BIGQUERY_TEST_PROJECT,
};
use surreal_sync::testing::surreal::{connect_auto, is_v3, SurrealConnection};
use surreal_sync::testing::{generate_test_id, TestConfig};
use surreal_sync_bigquery::from_bigquery::client::BigQueryClient;
use surreal_sync_bigquery::from_bigquery::{SourceOpts, SyncOpts, DEFAULT_PAGE_SIZE};

/// A booted emulator plus a SurrealDB target, wired for one test.
pub struct BigQueryTestEnv {
    pub test_id: u64,
    pub client: BigQueryClient,
    pub conn: SurrealConnection,
    pub config: TestConfig,
    pub api_endpoint: String,
    pub surreal_endpoint: String,
}

impl BigQueryTestEnv {
    /// Start (or reuse) the shared containers and connect to SurrealDB.
    pub async fn new(tables: Vec<String>, id_columns: Vec<String>) -> (Self, SourceOpts) {
        let emulator = shared_bigquery().await;
        let test_id = generate_test_id();

        let source_opts = SourceOpts {
            project_id: BIGQUERY_TEST_PROJECT.to_string(),
            dataset: BIGQUERY_TEST_DATASET.to_string(),
            job_project_id: None,
            credentials_json: None,
            location: None,
            api_endpoint: emulator.api_endpoint(),
            tables,
            id_columns,
            page_size: DEFAULT_PAGE_SIZE,
        };

        let client = BigQueryClient::new(&source_opts).expect("failed to build BigQuery client");

        let sdb = shared_surrealdb();
        let surreal_endpoint = sdb.ws_endpoint();
        let config = TestConfig::with_surreal_endpoint(test_id, &surreal_endpoint);
        let conn = connect_auto(&config)
            .await
            .expect("failed to connect to SurrealDB");

        (
            Self {
                test_id,
                client,
                conn,
                config,
                api_endpoint: emulator.api_endpoint(),
                surreal_endpoint,
            },
            source_opts,
        )
    }

    /// Run a statement against the emulator, failing the test on error.
    pub async fn execute(&self, sql: &str) {
        self.client
            .execute_query(sql)
            .await
            .unwrap_or_else(|e| panic!("BigQuery statement failed: {sql}\n{e}"));
    }

    /// Fully-qualified name for a table in the test dataset.
    pub fn qualified(&self, table: &str) -> String {
        format!("`{BIGQUERY_TEST_PROJECT}`.`{BIGQUERY_TEST_DATASET}`.`{table}`")
    }

    /// Ingest through the SurrealDB major version this environment connected to.
    pub async fn run_full_sync(&self, source_opts: &SourceOpts, sync_opts: &SyncOpts) -> usize {
        use surreal_sync_bigquery::from_bigquery::full_sync::run_full_sync;

        let ns = self.config.surreal_namespace.clone();
        let db = self.config.surreal_database.clone();

        if is_v3(&self.conn) {
            let opts = surreal_sync_surreal::v3::SurrealOpts {
                surreal_endpoint: self.surreal_endpoint.clone(),
                surreal_username: "root".to_string(),
                surreal_password: "root".to_string(),
            };
            let surreal = surreal_sync_surreal::v3::surreal_connect(&opts, &ns, &db)
                .await
                .expect("v3 sink connect failed");
            let sink = surreal_sync_surreal::v3::Surreal3Sink::new(surreal);
            run_full_sync(&self.client, &sink, source_opts, sync_opts)
                .await
                .expect("v3 ingestion failed")
        } else {
            let opts = surreal_sync_surreal::v2::SurrealOpts {
                surreal_endpoint: self.surreal_endpoint.clone(),
                surreal_username: "root".to_string(),
                surreal_password: "root".to_string(),
            };
            let surreal = surreal_sync_surreal::v2::surreal_connect(&opts, &ns, &db)
                .await
                .expect("v2 sink connect failed");
            let sink = surreal_sync_surreal::v2::Surreal2Sink::new(surreal);
            run_full_sync(&self.client, &sink, source_opts, sync_opts)
                .await
                .expect("v2 ingestion failed")
        }
    }

    /// Read every record of a SurrealDB table back as JSON, version-agnostically.
    ///
    /// `OMIT id` because a record ID deserializes as an enum, which
    /// `serde_json::Value` cannot represent. Tests that care about the record ID
    /// assert on it separately with [`Self::fetch_ids`].
    pub async fn fetch_rows(&self, table: &str) -> Vec<serde_json::Value> {
        self.fetch_rows_omitting(table, &["id"]).await
    }

    /// Like [`Self::fetch_rows`] but also omits columns whose SurrealDB type has
    /// no `serde_json` representation (notably `bytes`).
    pub async fn fetch_rows_omitting(&self, table: &str, omit: &[&str]) -> Vec<serde_json::Value> {
        let sql = format!("SELECT * OMIT {} FROM {table}", omit.join(", "));
        match &self.conn {
            SurrealConnection::V2(client) => {
                let mut resp = client.query(sql).await.expect("v2 query failed");
                resp.take(0).expect("v2 take failed")
            }
            SurrealConnection::V3(client) => {
                let mut resp = client.query(sql).await.expect("v3 query failed");
                resp.take(0).expect("v3 take failed")
            }
        }
    }

    /// Read one `bytes` column back, which `serde_json::Value` cannot represent.
    pub async fn fetch_bytes_column(&self, table: &str, column: &str) -> Vec<Vec<u8>> {
        let sql = format!("SELECT VALUE {column} FROM {table}");
        // The two SDK majors decode results through different traits: v2 goes via
        // serde (which hands bytes over in a shape `Vec<u8>` rejects, hence
        // [`AnyBytes`]), v3 via its own `SurrealValue`.
        match &self.conn {
            SurrealConnection::V2(client) => {
                let mut resp = client.query(sql).await.expect("v2 bytes query failed");
                let raw: Vec<AnyBytes> = resp.take(0).expect("v2 bytes take failed");
                raw.into_iter().map(|b| b.0).collect()
            }
            SurrealConnection::V3(client) => {
                let mut resp = client.query(sql).await.expect("v3 bytes query failed");
                resp.take::<Vec<Vec<u8>>>(0).expect("v3 bytes take failed")
            }
        }
    }

    /// Read the record IDs of a SurrealDB table as strings.
    pub async fn fetch_ids(&self, table: &str) -> Vec<String> {
        let sql = format!("SELECT VALUE type::string(id) FROM {table}");
        match &self.conn {
            SurrealConnection::V2(client) => {
                let mut resp = client.query(sql).await.expect("v2 id query failed");
                resp.take(0).expect("v2 id take failed")
            }
            SurrealConnection::V3(client) => {
                let mut resp = client.query(sql).await.expect("v3 id query failed");
                resp.take(0).expect("v3 id take failed")
            }
        }
    }
}

/// A byte string that deserializes from either serde representation.
///
/// The two SurrealDB SDK majors hand bytes to serde differently — one as a byte
/// buffer, the other as a sequence — and neither fits `Vec<u8>` on its own.
pub struct AnyBytes(pub Vec<u8>);

impl<'de> serde::Deserialize<'de> for AnyBytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = AnyBytes;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a byte string or a sequence of bytes")
            }

            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<AnyBytes, E> {
                Ok(AnyBytes(v.to_vec()))
            }

            fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<AnyBytes, E> {
                Ok(AnyBytes(v))
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<AnyBytes, E> {
                Ok(AnyBytes(v.as_bytes().to_vec()))
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<AnyBytes, A::Error> {
                let mut out = Vec::new();
                while let Some(byte) = seq.next_element::<u8>()? {
                    out.push(byte);
                }
                Ok(AnyBytes(out))
            }

            fn visit_newtype_struct<D: serde::Deserializer<'de>>(
                self,
                deserializer: D,
            ) -> Result<AnyBytes, D::Error> {
                deserializer.deserialize_any(Visitor)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// Default sync options for these tests.
pub fn sync_opts(batch_size: usize) -> SyncOpts {
    SyncOpts {
        batch_size,
        dry_run: false,
    }
}

/// A table name unique to one test run.
pub fn unique_table(prefix: &str, test_id: u64) -> String {
    format!("{prefix}_{test_id}")
}
