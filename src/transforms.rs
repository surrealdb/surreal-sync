//! Shared CLI helpers for `--transforms-config` and in-process Rust stages.
//!
//! Every `from *` sync command that has a sync path should call
//! [`load_transforms_from_args`] so identity vs configured transforms behave
//! the same across sources. Embedders that append Rust [`InPlaceTransform`]
//! stages should use [`merge_inplace_transforms`].

use anyhow::Context;
use std::path::Path;
use sync_transform::{ApplyOpts, InPlaceTransform, Pipeline};

/// Load `--transforms-config` into a [`Pipeline`] + [`ApplyOpts`].
///
/// **Omit flag vs empty / passthrough file — different buffering cadence:**
///
/// | CLI input | Pipeline | [`ApplyOpts`] |
/// |-----------|----------|---------------|
/// | Flag omitted (`None`) | Identity (no stage dispatch) | [`ApplyOpts::identity()`] (`batch_size = 1`, `max_in_flight = 1`) |
/// | Empty / passthrough-only TOML | Identity stages | [`ApplyOpts::default()`] (`batch_size = 1000`) |
///
/// Both yield an identity pipeline, but omit uses per-event CDC cadence
/// (`batch_size = 1`, `max_in_flight = 1`) while an empty/passthrough config
/// keeps config defaults (`batch_size = 1000`). That changes buffering cadence
/// even when no transform stages run. There is no separate oneshot write path
/// for identity — omit pins W=1 for CDC cadence only.
///
/// Bad TOML or unresolvable worker argv fails fast before sync starts (worker
/// spawn / argv fail-fast is also covered in `sync-transform` config tests).
pub fn load_transforms_from_args(
    transforms_config: Option<&Path>,
) -> anyhow::Result<(Pipeline, ApplyOpts)> {
    match transforms_config {
        None => {
            tracing::info!(
                "No --transforms-config; using identity transform pipeline (no stage dispatch)"
            );
            Ok((Pipeline::new(), ApplyOpts::identity()))
        }
        Some(path) => {
            let (pipeline, opts) = sync_transform::load_pipeline_and_opts(path)
                .with_context(|| format!("load --transforms-config {}", path.display()))?;
            if pipeline.is_identity() {
                tracing::info!(
                    path = %path.display(),
                    "Loaded --transforms-config but pipeline is identity (empty / passthrough-only)"
                );
            } else {
                tracing::info!(
                    path = %path.display(),
                    stages = pipeline.len(),
                    max_in_flight = opts.max_in_flight,
                    batch_size = opts.batch_size,
                    failure_policy = ?opts.failure_policy,
                    "Transform pipeline active from --transforms-config"
                );
            }
            Ok((pipeline, opts))
        }
    }
}

/// Load optional `--transforms-config`, then **append** in-process
/// [`InPlaceTransform`] stages (TOML stages run first).
///
/// When the flag is omitted, [`load_transforms_from_args`] yields
/// [`ApplyOpts::identity()`]. If any Rust stages are appended, opts are
/// upgraded to [`ApplyOpts::default()`] so batching is on — identity cadence
/// is only for “no stages”.
pub fn merge_inplace_transforms<I, T>(
    transforms_config: Option<&Path>,
    extra: I,
) -> anyhow::Result<(Pipeline, ApplyOpts)>
where
    I: IntoIterator<Item = T>,
    T: InPlaceTransform + 'static,
{
    merge_inplace_boxed(
        transforms_config,
        extra
            .into_iter()
            .map(|t| Box::new(t) as Box<dyn InPlaceTransform>),
    )
}

/// Like [`merge_inplace_transforms`], but accepts mixed concrete stage types via
/// [`Box<dyn InPlaceTransform>`] (needed for heterogeneous lists).
pub fn merge_inplace_boxed(
    transforms_config: Option<&Path>,
    extra: impl IntoIterator<Item = Box<dyn InPlaceTransform>>,
) -> anyhow::Result<(Pipeline, ApplyOpts)> {
    let (mut pipeline, mut apply_opts) = load_transforms_from_args(transforms_config)?;
    let before = pipeline.len();
    for stage in extra {
        pipeline.push_inplace_arc(std::sync::Arc::from(stage));
    }
    let added = pipeline.len().saturating_sub(before);
    if added > 0 && apply_opts == ApplyOpts::identity() {
        tracing::info!(
            added,
            "Rust InPlaceTransform stages appended after omitted --transforms-config; \
             upgrading ApplyOpts from identity to default (batching on)"
        );
        apply_opts = ApplyOpts::default();
    } else if added > 0 {
        tracing::info!(
            added,
            stages = pipeline.len(),
            "Appended Rust InPlaceTransform stages after --transforms-config"
        );
    }
    Ok((pipeline, apply_opts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use sync_transform::FlattenId;

    #[test]
    fn missing_path_returns_identity() {
        let (pipeline, opts) = load_transforms_from_args(None).expect("identity load");
        assert!(pipeline.is_identity());
        assert_eq!(opts, ApplyOpts::identity());
        assert_eq!(
            opts.max_in_flight, 1,
            "omit path must pin W=1 for CDC cadence"
        );
        assert_eq!(opts.batch_size, 1);
    }

    #[test]
    fn missing_file_fails_fast() {
        let err = load_transforms_from_args(Some(Path::new(
            "/nonexistent/surreal-sync-transforms-cli-xyz.toml",
        )))
        .expect_err("missing file must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("load --transforms-config"),
            "expected load context in error, got: {msg}"
        );
    }

    #[test]
    fn empty_toml_is_identity_pipeline_with_default_opts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.toml");
        std::fs::write(&path, "transforms = []\n").expect("write");
        let (pipeline, opts) = load_transforms_from_args(Some(&path)).expect("load");
        // Empty list → identity stages, but ApplyOpts come from config defaults
        // (batch_size 1000), not ApplyOpts::identity() (batch_size 1).
        assert!(pipeline.is_identity());
        assert_eq!(opts.batch_size, ApplyOpts::default().batch_size);
        assert_ne!(opts.batch_size, ApplyOpts::identity().batch_size);
        assert_eq!(opts, ApplyOpts::default());
    }

    #[test]
    fn bad_toml_fails_fast() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bad.toml");
        let mut f = std::fs::File::create(&path).expect("create");
        writeln!(f, "this is not [[transforms]] toml {{{{").expect("write");
        let err = load_transforms_from_args(Some(&path)).expect_err("bad TOML must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("load --transforms-config"),
            "expected load context in error, got: {msg}"
        );
    }

    /// Thin wrapper check: bad persistent worker argv fails through the CLI
    /// loader with the shared context string. Deeper spawn/resolvable coverage
    /// lives in `sync-transform` config tests.
    #[test]
    fn bad_persistent_worker_fails_fast_with_load_context() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bad-worker.toml");
        std::fs::write(
            &path,
            r#"
[[transforms]]
type = "command"
mode = "persistent"
command = ["/nonexistent/surreal-sync-transform-worker-cli-xyz"]
"#,
        )
        .expect("write");
        let err = load_transforms_from_args(Some(&path)).expect_err("bad worker must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("load --transforms-config"),
            "expected load context in error, got: {msg}"
        );
    }

    struct Tag;

    impl InPlaceTransform for Tag {
        fn transform(
            &self,
            _table: &str,
            _id: &mut sync_core::Value,
            fields: Option<&mut std::collections::HashMap<String, sync_core::Value>>,
        ) -> anyhow::Result<()> {
            if let Some(fields) = fields {
                fields.insert("tagged".into(), sync_core::Value::Bool(true));
            }
            Ok(())
        }
    }

    #[test]
    fn merge_rust_only_upgrades_identity_opts() {
        let (pipeline, opts) = merge_inplace_transforms(None, [Tag]).expect("merge rust-only");
        assert!(!pipeline.is_identity());
        assert_eq!(pipeline.len(), 1);
        assert_eq!(
            opts,
            ApplyOpts::default(),
            "Rust stages after omitted --transforms-config must upgrade off identity cadence"
        );
        assert_ne!(opts, ApplyOpts::identity());
    }

    #[test]
    fn merge_toml_flatten_id_then_rust() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("flatten.toml");
        std::fs::write(
            &path,
            r#"
[[transforms]]
type = "flatten_id"
"#,
        )
        .expect("write");
        let (pipeline, opts) =
            merge_inplace_transforms(Some(path.as_path()), [Tag]).expect("merge toml+rust");
        assert!(!pipeline.is_identity());
        assert_eq!(
            pipeline.len(),
            2,
            "TOML flatten_id then Rust stage → 2 stages"
        );
        // TOML load already uses default opts; upgrade rule must not clobber.
        assert_eq!(opts.batch_size, ApplyOpts::default().batch_size);
        let _ = FlattenId::default();
    }

    #[test]
    fn merge_no_extras_keeps_identity() {
        let (pipeline, opts) =
            merge_inplace_transforms(None, std::iter::empty::<Tag>()).expect("merge empty");
        assert!(pipeline.is_identity());
        assert_eq!(opts, ApplyOpts::identity());
    }

    #[test]
    fn merge_boxed_heterogeneous() {
        let (pipeline, opts) = merge_inplace_boxed(
            None,
            [
                Box::new(FlattenId::default()) as Box<dyn InPlaceTransform>,
                Box::new(Tag),
            ],
        )
        .expect("boxed merge");
        assert_eq!(pipeline.len(), 2);
        assert_eq!(opts, ApplyOpts::default());
    }
}
