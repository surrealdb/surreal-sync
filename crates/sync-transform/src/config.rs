//! TOML transform pipeline configuration.
//!
//! Empty / missing `[[transforms]]` yields an identity [`Pipeline`] (no stage
//! dispatch via [`Pipeline::is_identity`]). A lone `type = "passthrough"` stage
//! is collapsed to the same empty pipeline. Operators configure `command`
//! stages only — do not list `passthrough` alongside them.
//!
//! # Schema overview
//!
//! - **`[pipeline]`** — window / apply-runtime options shared by the whole
//!   sync (`batch_size`, `max_in_flight`, `failure_policy`, …). Named
//!   `pipeline` (not `apply`) so it is not confused with SurrealDB sink settings.
//! - **`[[transforms]]`** — ordered daisy-chained stages. Each `type = "command"`
//!   entry owns its own argv, stdio framer, timeout, and retry/backoff.

use crate::apply::{ApplyOpts, FailurePolicy};
use crate::external::{ChildStdioMode, ExternalTransform, RetryPolicy};
use crate::flatten_id::{FlattenId, DEFAULT_FLATTEN_ID_SEPARATOR};
use crate::framer::FramerKind;
use crate::pipeline::Pipeline;
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

/// Validated transform pipeline configuration (from TOML).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransformsConfig {
    /// Shared window / failure options (from `[pipeline]`).
    pub pipeline: PipelineSection,
    /// Real stages after collapsing passthrough-only entries.
    pub stages: Vec<ConfiguredStage>,
}

/// Pipeline-wide apply-window options (`[pipeline]` in TOML).
///
/// These are **not** SurrealDB sink settings — they control how surreal-sync
/// batches source events and overlaps transform work before ordered sink apply.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PipelineSection {
    /// Override for [`ApplyOpts::failure_policy`] when set.
    pub failure_policy: Option<FailurePolicy>,
    /// Override for [`ApplyOpts::batch_size`] when set.
    pub batch_size: Option<usize>,
    /// Override for [`ApplyOpts::batch_max_wait`] when set.
    pub batch_max_wait: Option<Duration>,
    /// Outer whole-pipeline transform timeout (covers all stages + retries).
    pub timeout: Option<Duration>,
    /// Override for [`ApplyOpts::max_in_flight`] when set.
    pub max_in_flight: Option<usize>,
}

/// One configured pipeline stage (passthrough never appears here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfiguredStage {
    /// Spawn a child worker and speak framed stdio (`type = "command"`).
    Command(CommandStageConfig),
    /// Flatten Array record IDs to Text (`type = "flatten_id"`).
    FlattenId(FlattenIdStageConfig),
}

/// Flatten-id stage settings from TOML (`type = "flatten_id"`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlattenIdStageConfig {
    /// Joiner between composite key parts (default `:`).
    pub separator: String,
}

/// Command-stage settings from TOML (`type = "command"`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandStageConfig {
    /// Argv to spawn (required).
    pub command: Vec<String>,
    /// Child lifecycle: persistent (default) or transient.
    pub mode: ChildStdioMode,
    /// Child-stdio framing settings.
    pub stdio: StdioConfig,
    /// Per-exchange timeout for this stage (optional).
    pub timeout: Option<Duration>,
    /// Per-stage retry/backoff (optional; default is a single attempt).
    pub retry: RetryPolicy,
}

/// Child-stdio framing (`stdio.*` under a command stage).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StdioConfig {
    pub framer: FramerKind,
}

impl TransformsConfig {
    /// Empty identity configuration.
    pub fn identity() -> Self {
        Self {
            pipeline: PipelineSection::default(),
            stages: Vec::new(),
        }
    }

    /// Whether this config yields an empty / identity pipeline.
    pub fn is_identity(&self) -> bool {
        self.stages.is_empty()
    }
}

/// Parse transforms TOML from a string.
///
/// Empty / whitespace-only input is identity. Missing or empty
/// `[[transforms]]` is identity. A lone passthrough stage collapses to
/// identity.
pub fn parse_transforms_toml(contents: &str) -> Result<TransformsConfig> {
    if contents.trim().is_empty() {
        return Ok(TransformsConfig::identity());
    }
    let raw: RawTransformsFile = toml::from_str(contents).context("parse transforms TOML")?;
    raw.try_into()
}

/// Load and parse a transforms TOML file from disk.
pub fn load_transforms_config(path: impl AsRef<Path>) -> Result<TransformsConfig> {
    let path = path.as_ref();
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("read transforms config {}", path.display()))?;
    parse_transforms_toml(&contents)
        .with_context(|| format!("parse transforms config {}", path.display()))
}

/// Build [`Pipeline`] + [`ApplyOpts`] from a config file path.
///
/// Spawns persistent external workers immediately (fail-fast on bad command).
pub fn load_pipeline_and_opts(path: impl AsRef<Path>) -> Result<(Pipeline, ApplyOpts)> {
    let cfg = load_transforms_config(path)?;
    let opts = ApplyOpts::from_transforms_config(&cfg);
    let pipeline = Pipeline::from_config(&cfg)?;
    Ok((pipeline, opts))
}

impl ApplyOpts {
    /// Derive apply options from `[pipeline]` (defaults when unset).
    pub fn from_transforms_config(cfg: &TransformsConfig) -> Self {
        let mut opts = ApplyOpts::default();
        let p = &cfg.pipeline;
        if let Some(policy) = p.failure_policy {
            opts.failure_policy = policy;
        }
        if let Some(n) = p.batch_size {
            opts.batch_size = n.max(1);
        }
        if let Some(d) = p.batch_max_wait {
            opts.batch_max_wait = d;
        }
        if let Some(d) = p.timeout {
            opts.timeout = d;
        }
        if let Some(n) = p.max_in_flight {
            opts.max_in_flight = n.max(1);
        }
        opts
    }
}

impl Pipeline {
    /// Build a pipeline from validated config.
    ///
    /// Empty config → identity ([`is_identity`](Self::is_identity) true).
    /// Command stages spawn child workers (persistent) or store argv (transient).
    /// Both modes resolve `command[0]` at config time so bad argv fails
    /// fast (transient would otherwise only fail on the first batch).
    pub fn from_config(cfg: &TransformsConfig) -> Result<Self> {
        let mut pipeline = Pipeline::new();
        for stage in &cfg.stages {
            match stage {
                ConfiguredStage::Command(cmd) => {
                    ensure_command_resolvable(&cmd.command)
                        .context("command not resolvable at config load")?;
                    let external = ExternalTransform::child_stdio(
                        cmd.mode,
                        cmd.command.clone(),
                        cmd.stdio.framer,
                    )
                    .context("create command transform from config")?
                    .with_timeout(cmd.timeout)
                    .with_retry(cmd.retry.clone());
                    pipeline.push_external(external);
                }
                ConfiguredStage::FlattenId(flat) => {
                    pipeline.push_inplace(FlattenId::new(flat.separator.clone()));
                }
            }
        }
        Ok(pipeline)
    }
}

/// Fail fast when `command[0]` is missing from PATH / filesystem.
///
/// Persistent workers also fail on spawn; this catches transient mode before
/// the first batch and gives a clearer error for both modes.
pub fn ensure_command_resolvable(command: &[String]) -> Result<()> {
    if command.is_empty() {
        bail!("command must not be empty");
    }
    let program = &command[0];
    let path = std::path::Path::new(program);
    if path.is_file() {
        return Ok(());
    }
    // Absolute / relative path that isn't a file → do not fall through to PATH.
    if program.contains('/') || program.contains('\\') {
        bail!(
            "command program not found as a file path: {program:?} \
             (full argv: {command:?})"
        );
    }
    // Bare program name: search PATH.
    if let Some(path_var) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path_var) {
            let candidate = dir.join(program);
            if candidate.is_file() {
                return Ok(());
            }
            #[cfg(windows)]
            {
                let with_exe = dir.join(format!("{program}.exe"));
                if with_exe.is_file() {
                    return Ok(());
                }
            }
        }
    }
    bail!(
        "command program not found on PATH: {program:?} \
         (full argv: {command:?})"
    );
}

/// Parse a human duration used in transforms TOML (`500ms`, `60s`, `1m`, `1h`).
///
/// Plain integers are seconds. Supported suffixes: `ms`, `s`, `m`, `h`
/// (case-insensitive).
pub fn parse_humantime(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        bail!("empty duration string");
    }
    let lower = s.to_ascii_lowercase();

    if let Some(num) = lower.strip_suffix("ms") {
        let n: u64 = num
            .trim()
            .parse()
            .with_context(|| format!("invalid milliseconds duration: {s}"))?;
        return Ok(Duration::from_millis(n));
    }
    if let Some(num) = lower.strip_suffix('s') {
        let n: u64 = num
            .trim()
            .parse()
            .with_context(|| format!("invalid seconds duration: {s}"))?;
        return Ok(Duration::from_secs(n));
    }
    if let Some(num) = lower.strip_suffix('m') {
        let n: u64 = num
            .trim()
            .parse()
            .with_context(|| format!("invalid minutes duration: {s}"))?;
        return Ok(Duration::from_secs(n.saturating_mul(60)));
    }
    if let Some(num) = lower.strip_suffix('h') {
        let n: u64 = num
            .trim()
            .parse()
            .with_context(|| format!("invalid hours duration: {s}"))?;
        return Ok(Duration::from_secs(n.saturating_mul(3600)));
    }

    let n: u64 = lower
        .parse()
        .with_context(|| format!("invalid duration (expected number or ms/s/m/h suffix): {s}"))?;
    Ok(Duration::from_secs(n))
}

// --- serde raw shapes -------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTransformsFile {
    #[serde(default)]
    pipeline: RawPipeline,
    #[serde(default)]
    transforms: Vec<RawStage>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPipeline {
    #[serde(default)]
    failure_policy: Option<RawFailurePolicy>,
    #[serde(default)]
    batch_size: Option<usize>,
    #[serde(default)]
    batch_max_wait: Option<String>,
    #[serde(default)]
    timeout: Option<String>,
    #[serde(default)]
    max_in_flight: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RawStage {
    Passthrough {},
    Command(RawCommandStage),
    FlattenId(RawFlattenIdStage),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFlattenIdStage {
    #[serde(default)]
    separator: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCommandStage {
    command: Option<Vec<String>>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    timeout: Option<String>,
    #[serde(default)]
    stdio: RawStdio,
    #[serde(default)]
    retry: RawRetry,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawFailurePolicy {
    Fail,
    Skip,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStdio {
    #[serde(default)]
    framer: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRetry {
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    initial_backoff: Option<String>,
    #[serde(default)]
    max_backoff: Option<String>,
    #[serde(default)]
    jitter: Option<bool>,
}

impl TryFrom<RawTransformsFile> for TransformsConfig {
    type Error = anyhow::Error;

    fn try_from(raw: RawTransformsFile) -> Result<Self> {
        let pipeline = validate_pipeline(raw.pipeline)?;
        let mut stages = Vec::new();
        for (i, stage) in raw.transforms.into_iter().enumerate() {
            match stage {
                RawStage::Passthrough {} => {
                    // Optional/unnecessary for operators; skipped so a lone
                    // passthrough collapses to identity.
                }
                RawStage::Command(raw_cmd) => {
                    let stage = validate_command(
                        i,
                        RawCommandFields {
                            command: raw_cmd.command,
                            mode: raw_cmd.mode,
                            timeout: raw_cmd.timeout,
                            stdio: raw_cmd.stdio,
                            retry: raw_cmd.retry,
                        },
                    )?;
                    stages.push(ConfiguredStage::Command(stage));
                }
                RawStage::FlattenId(raw) => {
                    let separator = raw
                        .separator
                        .unwrap_or_else(|| DEFAULT_FLATTEN_ID_SEPARATOR.to_string());
                    if separator.is_empty() {
                        bail!(
                            "transforms[{i}] (type = \"flatten_id\"): separator must not be empty"
                        );
                    }
                    stages.push(ConfiguredStage::FlattenId(FlattenIdStageConfig {
                        separator,
                    }));
                }
            }
        }
        Ok(TransformsConfig { pipeline, stages })
    }
}

fn validate_pipeline(raw: RawPipeline) -> Result<PipelineSection> {
    let ctx = || "pipeline";
    if let Some(0) = raw.batch_size {
        bail!("{}: batch_size must be >= 1", ctx());
    }
    if let Some(0) = raw.max_in_flight {
        bail!("{}: max_in_flight must be >= 1", ctx());
    }
    let batch_max_wait = raw
        .batch_max_wait
        .as_deref()
        .map(parse_humantime)
        .transpose()
        .with_context(|| format!("{}: invalid batch_max_wait", ctx()))?;
    let timeout = raw
        .timeout
        .as_deref()
        .map(parse_humantime)
        .transpose()
        .with_context(|| format!("{}: invalid timeout", ctx()))?;
    let failure_policy = raw.failure_policy.map(|p| match p {
        RawFailurePolicy::Fail => FailurePolicy::Fail,
        RawFailurePolicy::Skip => FailurePolicy::Skip,
    });
    Ok(PipelineSection {
        failure_policy,
        batch_size: raw.batch_size,
        batch_max_wait,
        timeout,
        max_in_flight: raw.max_in_flight,
    })
}

struct RawCommandFields {
    command: Option<Vec<String>>,
    mode: Option<String>,
    timeout: Option<String>,
    stdio: RawStdio,
    retry: RawRetry,
}

fn validate_command(index: usize, raw: RawCommandFields) -> Result<CommandStageConfig> {
    let ctx = || format!("transforms[{index}] (type = \"command\")");

    let command = raw
        .command
        .filter(|c| !c.is_empty())
        .with_context(|| format!("{}: command is required and must be non-empty", ctx()))?;

    let mode = match raw
        .mode
        .as_deref()
        .unwrap_or("persistent")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "persistent" => ChildStdioMode::Persistent,
        "transient" => ChildStdioMode::Transient,
        other => bail!(
            "{}: unsupported mode {other:?} (expected \"persistent\" or \"transient\")",
            ctx()
        ),
    };

    let framer = match raw
        .stdio
        .framer
        .as_deref()
        .unwrap_or("ndjson")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "ndjson" => FramerKind::Ndjson,
        other => bail!(
            "{}: unsupported stdio.framer {other:?} (v1 supports \"ndjson\")",
            ctx()
        ),
    };

    let timeout = raw
        .timeout
        .as_deref()
        .map(parse_humantime)
        .transpose()
        .with_context(|| format!("{}: invalid timeout", ctx()))?;

    let retry = validate_retry(&ctx, raw.retry)?;

    Ok(CommandStageConfig {
        command,
        mode,
        stdio: StdioConfig { framer },
        timeout,
        retry,
    })
}

fn validate_retry(ctx: &impl Fn() -> String, raw: RawRetry) -> Result<RetryPolicy> {
    let max_attempts = raw.max_attempts.unwrap_or(1);
    if max_attempts == 0 {
        bail!("{}: retry.max_attempts must be >= 1", ctx());
    }
    let initial_backoff = raw
        .initial_backoff
        .as_deref()
        .map(parse_humantime)
        .transpose()
        .with_context(|| format!("{}: invalid retry.initial_backoff", ctx()))?
        .unwrap_or(Duration::from_millis(200));
    let max_backoff = raw
        .max_backoff
        .as_deref()
        .map(parse_humantime)
        .transpose()
        .with_context(|| format!("{}: invalid retry.max_backoff", ctx()))?
        .unwrap_or(Duration::from_secs(30));
    if max_backoff < initial_backoff {
        bail!(
            "{}: retry.max_backoff ({max_backoff:?}) must be >= retry.initial_backoff ({initial_backoff:?})",
            ctx()
        );
    }
    Ok(RetryPolicy {
        max_attempts,
        initial_backoff,
        max_backoff,
        jitter: raw.jitter.unwrap_or(true),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_id_parses_with_default_separator() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "flatten_id"
"#,
        )
        .unwrap();
        assert_eq!(cfg.stages.len(), 1);
        match &cfg.stages[0] {
            ConfiguredStage::FlattenId(f) => assert_eq!(f.separator, ":"),
            other => panic!("expected FlattenId, got {other:?}"),
        }
        let pipeline = Pipeline::from_config(&cfg).unwrap();
        assert!(!pipeline.is_identity());
    }

    #[test]
    fn flatten_id_custom_separator() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "flatten_id"
separator = "_"
"#,
        )
        .unwrap();
        match &cfg.stages[0] {
            ConfiguredStage::FlattenId(f) => assert_eq!(f.separator, "_"),
            other => panic!("expected FlattenId, got {other:?}"),
        }
    }

    #[test]
    fn empty_string_is_identity() {
        let cfg = parse_transforms_toml("").unwrap();
        assert!(cfg.is_identity());
        let pipeline = Pipeline::from_config(&cfg).unwrap();
        assert!(pipeline.is_identity());
        assert_eq!(ApplyOpts::from_transforms_config(&cfg).max_in_flight, 1);
    }

    #[test]
    fn missing_transforms_key_is_identity() {
        let cfg = parse_transforms_toml("# comment only\n").unwrap();
        assert!(cfg.is_identity());
    }

    #[test]
    fn empty_transforms_array_is_identity() {
        let cfg = parse_transforms_toml("transforms = []\n").unwrap();
        assert!(cfg.is_identity());
    }

    #[test]
    fn passthrough_alone_collapses_to_identity() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "passthrough"
"#,
        )
        .unwrap();
        assert!(cfg.is_identity());
        let pipeline = Pipeline::from_config(&cfg).unwrap();
        assert!(pipeline.is_identity());
    }

    #[test]
    fn multiple_passthrough_collapses_to_identity() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "passthrough"

[[transforms]]
type = "passthrough"
"#,
        )
        .unwrap();
        assert!(cfg.is_identity());
    }

    #[test]
    fn command_only_parses_defaults() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "command"
command = ["kreuzberg-worker"]
"#,
        )
        .unwrap();
        assert!(!cfg.is_identity());
        assert_eq!(cfg.stages.len(), 1);
        let ConfiguredStage::Command(cmd) = &cfg.stages[0] else {
            panic!("expected Command stage");
        };
        assert_eq!(cmd.command, vec!["kreuzberg-worker"]);
        assert_eq!(cmd.mode, ChildStdioMode::Persistent);
        assert_eq!(cmd.stdio.framer, FramerKind::Ndjson);
        assert!(cmd.timeout.is_none());
        assert_eq!(cmd.retry.max_attempts, 1);
        assert!(cfg.pipeline.failure_policy.is_none());
        let opts = ApplyOpts::from_transforms_config(&cfg);
        assert_eq!(opts, ApplyOpts::default());
    }

    #[test]
    fn command_full_example_with_pipeline() {
        let cfg = parse_transforms_toml(
            r#"
[pipeline]
failure_policy = "fail"
batch_size = 1000
batch_max_wait = "500ms"
timeout = "120s"
max_in_flight = 2

[[transforms]]
type = "command"
command = ["kreuzberg-worker"]
mode = "persistent"
timeout = "60s"
stdio.framer = "ndjson"
retry.max_attempts = 5
retry.initial_backoff = "200ms"
retry.max_backoff = "30s"
retry.jitter = false
"#,
        )
        .unwrap();
        assert_eq!(cfg.pipeline.failure_policy, Some(FailurePolicy::Fail));
        assert_eq!(cfg.pipeline.batch_size, Some(1000));
        assert_eq!(
            cfg.pipeline.batch_max_wait,
            Some(Duration::from_millis(500))
        );
        assert_eq!(cfg.pipeline.timeout, Some(Duration::from_secs(120)));
        assert_eq!(cfg.pipeline.max_in_flight, Some(2));

        let ConfiguredStage::Command(cmd) = &cfg.stages[0] else {
            panic!("expected Command stage");
        };
        assert_eq!(cmd.mode, ChildStdioMode::Persistent);
        assert_eq!(cmd.timeout, Some(Duration::from_secs(60)));
        assert_eq!(cmd.retry.max_attempts, 5);
        assert_eq!(cmd.retry.initial_backoff, Duration::from_millis(200));
        assert_eq!(cmd.retry.max_backoff, Duration::from_secs(30));
        assert!(!cmd.retry.jitter);

        let opts = ApplyOpts::from_transforms_config(&cfg);
        assert_eq!(opts.failure_policy, FailurePolicy::Fail);
        assert_eq!(opts.batch_size, 1000);
        assert_eq!(opts.batch_max_wait, Duration::from_millis(500));
        assert_eq!(opts.timeout, Duration::from_secs(120));
        assert_eq!(opts.max_in_flight, 2);
    }

    #[test]
    fn two_command_stages_keep_distinct_argv_and_retry() {
        let cfg = parse_transforms_toml(
            r#"
[pipeline]
batch_size = 50
max_in_flight = 2

[[transforms]]
type = "command"
command = ["ocr-worker"]
mode = "persistent"
stdio.framer = "ndjson"
retry.max_attempts = 5
retry.initial_backoff = "200ms"

[[transforms]]
type = "command"
command = ["embed-worker", "--model", "x"]
mode = "transient"
stdio.framer = "ndjson"
timeout = "30s"
retry.max_attempts = 3
retry.initial_backoff = "100ms"
retry.max_backoff = "5s"
retry.jitter = false
"#,
        )
        .unwrap();
        assert_eq!(cfg.stages.len(), 2);
        let ConfiguredStage::Command(a) = &cfg.stages[0] else {
            panic!("expected Command stage");
        };
        let ConfiguredStage::Command(b) = &cfg.stages[1] else {
            panic!("expected Command stage");
        };
        assert_eq!(a.command, vec!["ocr-worker"]);
        assert_eq!(a.mode, ChildStdioMode::Persistent);
        assert_eq!(a.retry.max_attempts, 5);
        assert_eq!(b.command, vec!["embed-worker", "--model", "x"]);
        assert_eq!(b.mode, ChildStdioMode::Transient);
        assert_eq!(b.timeout, Some(Duration::from_secs(30)));
        assert_eq!(b.retry.max_attempts, 3);
        assert!(!b.retry.jitter);

        let opts = ApplyOpts::from_transforms_config(&cfg);
        assert_eq!(opts.batch_size, 50);
        assert_eq!(opts.max_in_flight, 2);
        // Pipeline opts come only from [pipeline], not last stage.
        assert_eq!(opts.failure_policy, FailurePolicy::Fail);
    }

    #[test]
    fn command_transient_and_pipeline_skip() {
        let cfg = parse_transforms_toml(
            r#"
[pipeline]
failure_policy = "skip"

[[transforms]]
type = "command"
mode = "transient"
command = ["worker", "--flag"]
"#,
        )
        .unwrap();
        let ConfiguredStage::Command(cmd) = &cfg.stages[0] else {
            panic!("expected Command stage");
        };
        assert_eq!(cmd.mode, ChildStdioMode::Transient);
        assert_eq!(cmd.command, vec!["worker", "--flag"]);
        let opts = ApplyOpts::from_transforms_config(&cfg);
        assert_eq!(opts.failure_policy, FailurePolicy::Skip);
    }

    #[test]
    fn passthrough_then_command_keeps_command_only() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "passthrough"

[[transforms]]
type = "command"
command = ["w"]
"#,
        )
        .unwrap();
        assert_eq!(cfg.stages.len(), 1);
        assert!(!cfg.is_identity());
    }

    #[test]
    fn rejects_missing_command() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "command"
"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("command"),
            "unexpected err: {err:#}"
        );
    }

    #[test]
    fn rejects_empty_command() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "command"
command = []
"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("command"),
            "unexpected err: {err:#}"
        );
    }

    #[test]
    fn rejects_old_external_type() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "external"
command = ["w"]
"#,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("external") || msg.contains("unknown") || msg.contains("did not match"),
            "unexpected err: {msg}"
        );
    }

    #[test]
    fn rejects_legacy_apply_section() {
        let err = parse_transforms_toml(
            r#"
[apply]
batch_size = 10

[[transforms]]
type = "command"
command = ["w"]
"#,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("apply") || msg.contains("unknown"),
            "unexpected err: {msg}"
        );
    }

    #[test]
    fn rejects_unknown_top_level_key() {
        let err = parse_transforms_toml(
            r#"
workers = 4

[[transforms]]
type = "command"
command = ["w"]
"#,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("workers") || msg.contains("unknown"),
            "unexpected err: {msg}"
        );
    }

    #[test]
    fn rejects_stdio_typo_key() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "command"
command = ["w"]
stdio.frameer = "ndjson"
"#,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("frameer") || msg.contains("unknown"),
            "unexpected err: {msg}"
        );
    }

    #[test]
    fn rejects_retry_typo_key() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "command"
command = ["w"]
retry.max_attemps = 3
"#,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("max_attemps") || msg.contains("unknown"),
            "unexpected err: {msg}"
        );
    }

    #[test]
    fn rejects_unknown_type() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "wasm"
"#,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("wasm") || msg.contains("unknown") || msg.contains("did not match"),
            "unexpected err: {msg}"
        );
    }

    #[test]
    fn rejects_bad_framer() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "command"
command = ["w"]
stdio.framer = "protobuf"
"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("framer"),
            "unexpected err: {err:#}"
        );
    }

    #[test]
    fn rejects_bad_mode() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "command"
command = ["w"]
mode = "always"
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("mode"), "unexpected err: {err:#}");
    }

    #[test]
    fn rejects_bad_failure_policy() {
        let err = parse_transforms_toml(
            r#"
[pipeline]
failure_policy = "retry"

[[transforms]]
type = "command"
command = ["w"]
"#,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("failure_policy") || msg.contains("retry"),
            "unexpected err: {msg}"
        );
    }

    #[test]
    fn rejects_zero_batch_size() {
        let err = parse_transforms_toml(
            r#"
[pipeline]
batch_size = 0

[[transforms]]
type = "command"
command = ["w"]
"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("batch_size"),
            "unexpected err: {err:#}"
        );
    }

    #[test]
    fn rejects_zero_retry_attempts() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "command"
command = ["w"]
retry.max_attempts = 0
"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("max_attempts"),
            "unexpected err: {err:#}"
        );
    }

    #[test]
    fn rejects_invalid_duration() {
        let err = parse_transforms_toml(
            r#"
[pipeline]
timeout = "nope"

[[transforms]]
type = "command"
command = ["w"]
"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("timeout"),
            "unexpected err: {err:#}"
        );
    }

    #[test]
    fn parse_humantime_variants() {
        assert_eq!(
            parse_humantime("500ms").unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(parse_humantime("60s").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_humantime("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_humantime("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_humantime("45").unwrap(), Duration::from_secs(45));
        assert_eq!(parse_humantime("1S").unwrap(), Duration::from_secs(1));
    }

    #[test]
    fn transient_missing_command_fails_at_config_time() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "command"
mode = "transient"
command = ["/nonexistent/surreal-sync-transform-worker-xyz"]
"#,
        )
        .unwrap();
        let err = Pipeline::from_config(&cfg).expect_err("missing transient argv must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("not found") || msg.contains("resolvable"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn rejects_stage_level_pipeline_keys() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "command"
command = ["w"]
batch_size = 10
"#,
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("batch_size") || msg.contains("unknown") || msg.contains("did not match"),
            "unexpected err: {msg}"
        );
    }

    #[test]
    fn ensure_command_resolvable_rejects_empty() {
        let err = ensure_command_resolvable(&[]).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn load_transforms_config_missing_file_fails_fast() {
        let err = load_transforms_config("/nonexistent/surreal-sync-transforms-xyz.toml")
            .expect_err("missing file must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("read transforms config") || msg.contains("No such file"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn load_transforms_config_bad_toml_fails_fast() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "surreal-sync-bad-transforms-{}.toml",
            std::process::id()
        ));
        std::fs::write(&path, "[[transforms]]\ntype = \"command\"\ncommand = [\n")
            .expect("write temp");
        let err = load_transforms_config(&path).expect_err("bad TOML must fail");
        let _ = std::fs::remove_file(&path);
        let msg = format!("{err:#}");
        assert!(
            msg.contains("parse transforms config") || msg.contains("TOML"),
            "unexpected: {msg}"
        );
    }
}
