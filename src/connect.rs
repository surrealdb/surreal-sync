use crate::SurrealOpts;

// Connect to SurrealDB instance
pub async fn connect_to_surrealdb(
    to_opts: &SurrealOpts,
    to_namespace: String,
    to_database: String,
) -> anyhow::Result<surrealdb::Surreal<surrealdb::engine::any::Any>> {
    let surreal_endpoint = to_opts
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");
    let surreal = surrealdb::engine::any::connect(surreal_endpoint).await?;

    surreal
        .signin(surrealdb::opt::auth::Root {
            username: &to_opts.surreal_username,
            password: &to_opts.surreal_password,
        })
        .await?;

    surreal.use_ns(&to_namespace).use_db(&to_database).await?;

    Ok(surreal)
}
