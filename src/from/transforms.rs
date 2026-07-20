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
/// Missing path → identity pipeline with `batch_size = 1` (skips transform
/// stage dispatch; matches pre-transform apply cadence). Bad TOML or
/// unresolvable worker argv fails fast before sync starts.
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
        assert_eq!(opts.batch_size, ApplyOpts::identity().batch_size);
        assert_eq!(opts.max_in_flight, ApplyOpts::identity().max_in_flight);
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
    fn empty_toml_is_identity_pipeline() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.toml");
        std::fs::write(&path, "transforms = []\n").expect("write");
        let (pipeline, _opts) = load_transforms_from_args(Some(&path)).expect("load");
        // Empty list collapses to identity stages; ApplyOpts still come from the
        // loaded config defaults (not ApplyOpts::identity()'s batch_size = 1).
        assert!(pipeline.is_identity());
    }

    #[test]
    fn bad_toml_fails_fast() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bad.toml");
        let mut f = std::fs::File::create(&path).expect("create");
        write!(f, "this is not [[transforms]] toml {{{{\n").expect("write");
        let err = load_transforms_from_args(Some(&path)).expect_err("bad TOML must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("load --transforms-config"),
            "expected load context in error, got: {msg}"
        );
    }
}
