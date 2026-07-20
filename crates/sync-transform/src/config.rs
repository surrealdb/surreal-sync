//! TOML transform pipeline configuration.
//!
//! Empty / missing `[[transforms]]` yields an identity [`Pipeline`] (no stage
//! dispatch via [`Pipeline::is_identity`]). A lone `type = "passthrough"` stage
//! is collapsed to the same empty pipeline. Operators configure `external`
//! stages only — do not list `passthrough` alongside them.

use crate::apply::{ApplyOpts, FailurePolicy};
use crate::external::{ChildStdioMode, ExternalTransform};
use crate::pipeline::Pipeline;
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::path::Path;
use std::time::Duration;

/// Validated transform pipeline configuration (from TOML).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransformsConfig {
    /// Real stages after collapsing passthrough-only entries.
    pub stages: Vec<ConfiguredStage>,
}

/// One configured pipeline stage (passthrough never appears here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfiguredStage {
    /// Child-stdio external worker.
    External(ExternalStageConfig),
}

/// External stage settings from TOML.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalStageConfig {
    /// Override for [`ApplyOpts::failure_policy`] when set.
    pub failure_policy: Option<FailurePolicy>,
    /// Override for [`ApplyOpts::batch_size`] when set.
    pub batch_size: Option<usize>,
    /// Override for [`ApplyOpts::batch_max_wait`] when set.
    pub batch_max_wait: Option<Duration>,
    /// Override for [`ApplyOpts::timeout`] when set.
    pub timeout: Option<Duration>,
    /// Override for [`ApplyOpts::max_in_flight`] when set.
    pub max_in_flight: Option<usize>,
    /// Transport kind (v1: stdin only).
    pub transport: TransportKind,
    /// Child-stdio settings.
    pub stdin: StdinConfig,
}

/// Supported external transports (v1: child-process stdio only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransportKind {
    /// Child-process stdin/stdout pipes (`transport = "stdin"`).
    #[default]
    Stdin,
}

/// Supported wire framers (v1: NDJSON only).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FramerKind {
    /// Newline-delimited JSON (`stdin.framer = "ndjson"`).
    #[default]
    Ndjson,
}

/// Child-stdio worker settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StdinConfig {
    pub mode: ChildStdioMode,
    pub command: Vec<String>,
    pub framer: FramerKind,
}

impl TransformsConfig {
    /// Empty identity configuration.
    pub fn identity() -> Self {
        Self { stages: Vec::new() }
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
    /// Derive apply options from transform config (defaults when identity / unset).
    ///
    /// When multiple external stages set the same field, the **last** wins.
    pub fn from_transforms_config(cfg: &TransformsConfig) -> Self {
        let mut opts = ApplyOpts::default();
        for stage in &cfg.stages {
            let ConfiguredStage::External(ext) = stage;
            if let Some(policy) = ext.failure_policy {
                opts.failure_policy = policy;
            }
            if let Some(n) = ext.batch_size {
                opts.batch_size = n.max(1);
            }
            if let Some(d) = ext.batch_max_wait {
                opts.batch_max_wait = d;
            }
            if let Some(d) = ext.timeout {
                opts.timeout = d;
            }
            if let Some(n) = ext.max_in_flight {
                opts.max_in_flight = n.max(1);
            }
        }
        opts
    }
}

impl Pipeline {
    /// Build a pipeline from validated config.
    ///
    /// Empty config → identity ([`is_identity`](Self::is_identity) true).
    /// External stages spawn child workers (persistent) or store argv (transient).
    /// Both modes resolve `stdin.command[0]` at config time so bad argv fails
    /// fast (transient would otherwise only fail on the first batch).
    pub fn from_config(cfg: &TransformsConfig) -> Result<Self> {
        let mut pipeline = Pipeline::new();
        for stage in &cfg.stages {
            match stage {
                ConfiguredStage::External(ext) => {
                    if ext.transport != TransportKind::Stdin {
                        bail!("unsupported transform transport (v1 supports stdin only)");
                    }
                    if ext.stdin.framer != FramerKind::Ndjson {
                        bail!("unsupported stdin.framer (v1 supports ndjson only)");
                    }
                    ensure_command_resolvable(&ext.stdin.command)
                        .context("stdin.command not resolvable at config load")?;
                    let external = ExternalTransform::child_stdio(
                        ext.stdin.mode,
                        ext.stdin.command.clone(),
                    )
                    .context("create external transform from config")?;
                    pipeline.push_external(external);
                }
            }
        }
        Ok(pipeline)
    }
}

/// Fail fast when `stdin.command[0]` is missing from PATH / filesystem.
///
/// Persistent workers also fail on spawn; this catches transient mode before
/// the first batch and gives a clearer error for both modes.
pub fn ensure_command_resolvable(command: &[String]) -> Result<()> {
    if command.is_empty() {
        bail!("stdin.command must not be empty");
    }
    let program = &command[0];
    let path = std::path::Path::new(program);
    if path.is_file() {
        return Ok(());
    }
    // Absolute / relative path that isn't a file → do not fall through to PATH.
    if program.contains('/') || program.contains('\\') {
        bail!(
            "stdin.command program not found as a file path: {program:?} \
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
        "stdin.command program not found on PATH: {program:?} \
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
struct RawTransformsFile {
    #[serde(default)]
    transforms: Vec<RawStage>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RawStage {
    Passthrough {},
    External {
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
        #[serde(default = "default_transport")]
        transport: String,
        #[serde(default)]
        stdin: RawStdin,
    },
}

fn default_transport() -> String {
    "stdin".to_string()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RawFailurePolicy {
    Fail,
    Skip,
}

#[derive(Debug, Default, Deserialize)]
struct RawStdin {
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    command: Option<Vec<String>>,
    #[serde(default)]
    framer: Option<String>,
}

impl TryFrom<RawTransformsFile> for TransformsConfig {
    type Error = anyhow::Error;

    fn try_from(raw: RawTransformsFile) -> Result<Self> {
        let mut stages = Vec::new();
        for (i, stage) in raw.transforms.into_iter().enumerate() {
            match stage {
                RawStage::Passthrough {} => {
                    // Optional/unnecessary for operators; skipped so a lone
                    // passthrough collapses to identity.
                }
                RawStage::External {
                    failure_policy,
                    batch_size,
                    batch_max_wait,
                    timeout,
                    max_in_flight,
                    transport,
                    stdin,
                } => {
                    let stage = validate_external(
                        i,
                        RawExternalFields {
                            failure_policy,
                            batch_size,
                            batch_max_wait,
                            timeout,
                            max_in_flight,
                            transport,
                            stdin,
                        },
                    )?;
                    stages.push(ConfiguredStage::External(stage));
                }
            }
        }
        Ok(TransformsConfig { stages })
    }
}

struct RawExternalFields {
    failure_policy: Option<RawFailurePolicy>,
    batch_size: Option<usize>,
    batch_max_wait: Option<String>,
    timeout: Option<String>,
    max_in_flight: Option<usize>,
    transport: String,
    stdin: RawStdin,
}

fn validate_external(index: usize, raw: RawExternalFields) -> Result<ExternalStageConfig> {
    let ctx = || format!("transforms[{index}] (type = \"external\")");

    let transport = match raw.transport.trim().to_ascii_lowercase().as_str() {
        "stdin" => TransportKind::Stdin,
        other => bail!("{}: unsupported transport {other:?} (v1 supports \"stdin\")", ctx()),
    };

    let command = raw
        .stdin
        .command
        .filter(|c| !c.is_empty())
        .with_context(|| format!("{}: stdin.command is required and must be non-empty", ctx()))?;

    let mode = match raw
        .stdin
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
            "{}: unsupported stdin.mode {other:?} (expected \"persistent\" or \"transient\")",
            ctx()
        ),
    };

    let framer = match raw
        .stdin
        .framer
        .as_deref()
        .unwrap_or("ndjson")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "ndjson" => FramerKind::Ndjson,
        other => bail!(
            "{}: unsupported stdin.framer {other:?} (v1 supports \"ndjson\")",
            ctx()
        ),
    };

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

    Ok(ExternalStageConfig {
        failure_policy,
        batch_size: raw.batch_size,
        batch_max_wait,
        timeout,
        max_in_flight: raw.max_in_flight,
        transport,
        stdin: StdinConfig {
            mode,
            command,
            framer,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn external_only_parses_defaults() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "external"
stdin.command = ["kreuzberg-worker"]
"#,
        )
        .unwrap();
        assert!(!cfg.is_identity());
        assert_eq!(cfg.stages.len(), 1);
        let ConfiguredStage::External(ext) = &cfg.stages[0];
        assert_eq!(ext.stdin.command, vec!["kreuzberg-worker"]);
        assert_eq!(ext.stdin.mode, ChildStdioMode::Persistent);
        assert_eq!(ext.stdin.framer, FramerKind::Ndjson);
        assert_eq!(ext.transport, TransportKind::Stdin);
        assert!(ext.failure_policy.is_none());
        let opts = ApplyOpts::from_transforms_config(&cfg);
        assert_eq!(opts, ApplyOpts::default());
    }

    #[test]
    fn external_full_example_from_plan() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "external"
failure_policy = "fail"
batch_size = 1000
batch_max_wait = "500ms"
timeout = "60s"
max_in_flight = 2
transport = "stdin"
stdin.mode = "persistent"
stdin.command = ["kreuzberg-worker"]
stdin.framer = "ndjson"
"#,
        )
        .unwrap();
        let ConfiguredStage::External(ext) = &cfg.stages[0];
        assert_eq!(ext.failure_policy, Some(FailurePolicy::Fail));
        assert_eq!(ext.batch_size, Some(1000));
        assert_eq!(ext.batch_max_wait, Some(Duration::from_millis(500)));
        assert_eq!(ext.timeout, Some(Duration::from_secs(60)));
        assert_eq!(ext.max_in_flight, Some(2));
        assert_eq!(ext.stdin.mode, ChildStdioMode::Persistent);

        let opts = ApplyOpts::from_transforms_config(&cfg);
        assert_eq!(opts.failure_policy, FailurePolicy::Fail);
        assert_eq!(opts.batch_size, 1000);
        assert_eq!(opts.batch_max_wait, Duration::from_millis(500));
        assert_eq!(opts.timeout, Duration::from_secs(60));
        assert_eq!(opts.max_in_flight, 2);
    }

    #[test]
    fn external_transient_and_skip() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "external"
failure_policy = "skip"
stdin.mode = "transient"
stdin.command = ["worker", "--flag"]
"#,
        )
        .unwrap();
        let ConfiguredStage::External(ext) = &cfg.stages[0];
        assert_eq!(ext.failure_policy, Some(FailurePolicy::Skip));
        assert_eq!(ext.stdin.mode, ChildStdioMode::Transient);
        assert_eq!(ext.stdin.command, vec!["worker", "--flag"]);
        let opts = ApplyOpts::from_transforms_config(&cfg);
        assert_eq!(opts.failure_policy, FailurePolicy::Skip);
    }

    #[test]
    fn passthrough_then_external_keeps_external_only() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "passthrough"

[[transforms]]
type = "external"
stdin.command = ["w"]
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
type = "external"
"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("stdin.command"),
            "unexpected err: {err:#}"
        );
    }

    #[test]
    fn rejects_empty_command() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "external"
stdin.command = []
"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("stdin.command"),
            "unexpected err: {err:#}"
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
    fn rejects_bad_transport() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "external"
transport = "http"
stdin.command = ["w"]
"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("transport"),
            "unexpected err: {err:#}"
        );
    }

    #[test]
    fn rejects_bad_framer() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "external"
stdin.command = ["w"]
stdin.framer = "protobuf"
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
type = "external"
stdin.command = ["w"]
stdin.mode = "always"
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("mode"), "unexpected err: {err:#}");
    }

    #[test]
    fn rejects_bad_failure_policy() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "external"
failure_policy = "retry"
stdin.command = ["w"]
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
[[transforms]]
type = "external"
batch_size = 0
stdin.command = ["w"]
"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("batch_size"),
            "unexpected err: {err:#}"
        );
    }

    #[test]
    fn rejects_invalid_duration() {
        let err = parse_transforms_toml(
            r#"
[[transforms]]
type = "external"
timeout = "nope"
stdin.command = ["w"]
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
        assert_eq!(parse_humantime("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_humantime("60s").unwrap(), Duration::from_secs(60));
        assert_eq!(parse_humantime("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_humantime("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_humantime("45").unwrap(), Duration::from_secs(45));
        assert_eq!(parse_humantime("1S").unwrap(), Duration::from_secs(1));
    }

    #[test]
    fn later_external_opts_override() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "external"
batch_size = 10
max_in_flight = 1
stdin.command = ["a"]

[[transforms]]
type = "external"
batch_size = 20
failure_policy = "skip"
stdin.command = ["b"]
"#,
        )
        .unwrap();
        let opts = ApplyOpts::from_transforms_config(&cfg);
        assert_eq!(opts.batch_size, 20);
        assert_eq!(opts.max_in_flight, 1);
        assert_eq!(opts.failure_policy, FailurePolicy::Skip);
    }

    #[test]
    fn transient_missing_command_fails_at_config_time() {
        let cfg = parse_transforms_toml(
            r#"
[[transforms]]
type = "external"
stdin.mode = "transient"
stdin.command = ["/nonexistent/surreal-sync-transform-worker-xyz"]
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
        std::fs::write(&path, "[[transforms]]\ntype = \"external\"\nstdin.command = [\n")
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
