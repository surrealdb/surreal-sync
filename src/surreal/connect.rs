use crate::SurrealOpts;

pub async fn surreal_connect(
    opts: &SurrealOpts,
    ns: &str,
    db: &str,
) -> anyhow::Result<surrealdb::Surreal<surrealdb::engine::any::Any>> {
    // Connect to SurrealDB
    let surreal_endpoint = opts
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");
    let surreal = surrealdb::engine::any::connect(surreal_endpoint).await?;

    surreal
        .signin(surrealdb::opt::auth::Root {
            username: &opts.surreal_username,
            password: &opts.surreal_password,
        })
        .await?;

    surreal.use_ns(ns).use_db(db).await?;

    Ok(surreal)
}
