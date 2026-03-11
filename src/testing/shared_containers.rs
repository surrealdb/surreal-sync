//! Shared Docker containers for integration tests.
//!
//! Each container type is lazily initialized once per test binary process.
//! Tests share the container but use unique namespaces/databases for isolation.
//! An atexit handler cleans up all shared containers when the process exits.

use std::sync::{Mutex, OnceLock};
use tokio::sync::OnceCell;

use surreal_version::testing::SurrealDbContainer;
use surreal_sync_postgresql::testing::container::PostgresContainer;

/// Track container names for cleanup at process exit.
static CONTAINER_NAMES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

fn register_container(name: &str) {
    let names = CONTAINER_NAMES.get_or_init(|| {
        // Register atexit handler on first container creation
        extern "C" fn cleanup() {
            if let Some(names) = CONTAINER_NAMES.get() {
                if let Ok(names) = names.lock() {
                    for name in names.iter() {
                        let _ = std::process::Command::new("docker")
                            .args(["rm", "-f", name])
                            .stdout(std::process::Stdio::null())
                            .stderr(std::process::Stdio::null())
                            .status();
                    }
                }
            }
        }
        unsafe { libc::atexit(cleanup) };
        Mutex::new(Vec::new())
    });
    if let Ok(mut names) = names.lock() {
        names.push(name.to_string());
    }
}

/// Returns a shared SurrealDB container, starting it on first call.
pub fn shared_surrealdb() -> &'static SurrealDbContainer {
    static DB: OnceLock<SurrealDbContainer> = OnceLock::new();
    DB.get_or_init(|| {
        let name = format!("shared-sdb-{}", std::process::id());
        register_container(&name);
        let mut c = SurrealDbContainer::new(&name);
        c.start().expect("shared SurrealDB failed to start");
        c.wait_until_ready(30)
            .expect("shared SurrealDB not ready in 30s");
        c
    })
}

/// Returns a shared PostgreSQL container (with wal2json), starting it on first call.
pub async fn shared_postgres() -> &'static PostgresContainer {
    static PG: OnceCell<PostgresContainer> = OnceCell::const_new();
    PG.get_or_init(|| async {
        let name = format!("shared-pg-{}", std::process::id());
        register_container(&name);
        let mut c = PostgresContainer::new(&name);
        c.build_image().expect("Postgres build_image failed");
        c.start().expect("Postgres start failed");
        c.wait_until_ready(30)
            .await
            .expect("Postgres not ready in 30s");
        c
    })
    .await
}

/// Create a fresh PostgreSQL database for a test and return its connection string.
pub async fn create_postgres_test_db(
    container: &PostgresContainer,
    test_id: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    let db_name = format!("test_{test_id}");
    let admin_conn_str = &container.connection_string;

    let (client, conn) =
        tokio_postgres::connect(admin_conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("admin PG connection error: {e}");
        }
    });

    match client
        .execute(&format!("CREATE DATABASE \"{db_name}\""), &[])
        .await
    {
        Ok(_) => {}
        Err(e) if e.to_string().contains("already exists") => {}
        Err(e) => return Err(e.into()),
    }

    let test_conn = admin_conn_str
        .replace("dbname=testdb", &format!("dbname={db_name}"));
    Ok(test_conn)
}

/// Returns a shared MySQL container, starting it on first call.
pub async fn shared_mysql() -> &'static surreal_sync_mysql_trigger_source::testing::MySQLContainer {
    static MY: OnceCell<surreal_sync_mysql_trigger_source::testing::MySQLContainer> =
        OnceCell::const_new();
    MY.get_or_init(|| async {
        let name = format!("shared-mysql-{}", std::process::id());
        register_container(&name);
        let mut c = surreal_sync_mysql_trigger_source::testing::MySQLContainer::new(&name);
        c.start().expect("MySQL start failed");
        c.wait_until_ready(30)
            .await
            .expect("MySQL not ready in 30s");
        c
    })
    .await
}

/// Create a fresh MySQL database for a test.
pub async fn create_mysql_test_db(
    container: &surreal_sync_mysql_trigger_source::testing::MySQLContainer,
    test_id: u64,
) -> Result<String, Box<dyn std::error::Error>> {
    let db_name = format!("test_{test_id}");
    let pool = mysql_async::Pool::from_url(&container.connection_string)?;
    let mut conn = pool.get_conn().await?;
    use mysql_async::prelude::Queryable;
    conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS `{db_name}`"))
        .await?;
    drop(conn);
    pool.disconnect().await?;

    let test_conn = container
        .connection_string
        .replace("/testdb", &format!("/{db_name}"));
    Ok(test_conn)
}

/// Returns a shared MongoDB container, starting it on first call.
pub async fn shared_mongodb() -> &'static crate::testing::mongodb_container::MongoContainer {
    static MG: OnceCell<crate::testing::mongodb_container::MongoContainer> =
        OnceCell::const_new();
    MG.get_or_init(|| async {
        let name = format!("shared-mongo-{}", std::process::id());
        register_container(&name);
        let mut c = crate::testing::mongodb_container::MongoContainer::new(&name);
        c.start().expect("MongoDB start failed");
        c.wait_until_ready(30)
            .await
            .expect("MongoDB not ready in 30s");
        c
    })
    .await
}

/// Returns a shared Neo4j container, starting it on first call.
pub async fn shared_neo4j() -> &'static surreal_sync_neo4j_source::testing::container::Neo4jContainer
{
    static N4: OnceCell<surreal_sync_neo4j_source::testing::container::Neo4jContainer> =
        OnceCell::const_new();
    N4.get_or_init(|| async {
        let name = format!("shared-neo4j-{}", std::process::id());
        register_container(&name);
        let mut c =
            surreal_sync_neo4j_source::testing::container::Neo4jContainer::new(&name);
        c.start().expect("Neo4j start failed");
        c.wait_until_ready(30)
            .await
            .expect("Neo4j not ready in 30s");
        c
    })
    .await
}

// Kafka container shared accessor is defined inline in the kafka test binary
// because the surreal_sync_kafka_producer crate has linking constraints.
