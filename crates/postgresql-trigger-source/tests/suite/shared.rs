use surreal_sync_postgresql::testing::container::PostgresContainer;
use tokio::sync::{Mutex as TokioMutex, MutexGuard, OnceCell};

static PG: OnceCell<PostgresContainer> = OnceCell::const_new();
static SHARED_DB_LOCK: TokioMutex<()> = TokioMutex::const_new(());

fn register_cleanup(name: &str) {
    use std::sync::{Mutex, OnceLock};
    static NAMES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    let names = NAMES.get_or_init(|| {
        extern "C" fn cleanup() {
            let _ = std::process::Command::new("docker")
                .args([
                    "rm",
                    "-f",
                    &format!("shared-trigger-pg-{}", std::process::id()),
                ])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
        unsafe { libc::atexit(cleanup) };
        Mutex::new(Vec::new())
    });
    if let Ok(mut n) = names.lock() {
        n.push(name.to_string());
    }
}

pub async fn postgres() -> &'static PostgresContainer {
    PG.get_or_init(|| async {
        let name = format!("shared-trigger-pg-{}", std::process::id());
        register_cleanup(&name);
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

/// Serialize tests that mutate the container default `testdb` (shared
/// `surreal_sync_changes` / DDL). Prefer [`create_test_db`] for parallel isolation.
pub async fn lock_shared_db() -> MutexGuard<'static, ()> {
    SHARED_DB_LOCK.lock().await
}

/// Create a unique database within the shared container for test isolation.
pub async fn create_test_db(
    container: &PostgresContainer,
    db_name: &str,
) -> anyhow::Result<String> {
    let (client, conn) =
        tokio_postgres::connect(&container.connection_string, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });

    match client
        .execute(&format!("CREATE DATABASE \"{db_name}\""), &[])
        .await
    {
        Ok(_) => {}
        Err(e) if e.to_string().contains("already exists") => {}
        Err(e) => return Err(e.into()),
    }

    let test_conn = container
        .connection_string
        .replace("dbname=testdb", &format!("dbname={db_name}"));
    Ok(test_conn)
}
