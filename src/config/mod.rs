//! Configuration utilities.
//!
//! - [`duration`]: Duration string parsing helpers.
//! - [`file`]: TOML config file support (`-c` / `--config-file`). Defines the generic
//!   [`ConfigFile<S>`](file::ConfigFile) wrapper, [`SinkConfig`](file::SinkConfig), and
//!   [`load_config`] for reading TOML files. Source-specific config structs live in
//!   their respective crates (e.g., `surreal_sync_postgresql_trigger_source::toml_config`).

mod duration;
pub mod file;

pub use duration::parse_duration_to_secs;
pub use file::load_config;
