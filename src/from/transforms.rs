//! Shared CLI helpers for `--transforms-config`.
//!
//! Every `from *` sync command that has a sync path should call
//! [`load_transforms_from_args`] so identity vs configured transforms behave
//! the same across sources.

use anyhow::Context;
use std::path::Path;
use sync_transform::{ApplyOpts, Pipeline};

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn missing_path_returns_identity() {
        let (pipeline, opts) = load_transforms_from_args(None).expect("identity load");
        assert!(pipeline.is_identity());
        assert_eq!(opts, ApplyOpts::identity());
        assert_eq!(opts.max_in_flight, 1, "omit path must pin W=1 for CDC cadence");
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
}
