use surreal_sync_postgresql::testing::container::PostgresContainer;
use tokio::sync::OnceCell;

static PG: OnceCell<PostgresContainer> = OnceCell::const_new();

fn register_cleanup(name: &str) {
    use std::sync::{Mutex, OnceLock};
    static NAMES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    let names = NAMES.get_or_init(|| {
        extern "C" fn cleanup() {
            let _ = std::process::Command::new("docker")
                .args(["rm", "-f", &format!("shared-trigger-pg-{}", std::process::id())])
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
