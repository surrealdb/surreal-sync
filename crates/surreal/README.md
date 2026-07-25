# surreal-sync-surreal

SurrealDB sink for surreal-sync. Pair this with a source crate (MySQL, Snowflake, JSONL, and so on).

## Depend

Default is SurrealDB **3**:

```toml
surreal-sync-surreal = "0.6"
```

For SurrealDB **2**:

```toml
surreal-sync-surreal = { version = "0.6", default-features = false, features = ["v2"] }
```

Apps should enable one major version. The `surreal-sync` CLI enables both so it can detect the server version.

## Use in an embedder

```rust
use surreal_sync_surreal::Surreal3Sink;
// or: use surreal_sync_surreal::Surreal2Sink;

// Pass the sink type to your source crate's run():
// source_crate::run::<Surreal3Sink>(transforms).await?;
```

See the `examples/from-*` packages in the [surreal-sync](https://github.com/surrealdb/surreal-sync) repository.
