//! Common utilities for sync handlers.

mod schema;
mod sdk_version;
mod sink;

pub use schema::{
    extract_json_fields_from_schema, extract_postgresql_database, load_schema_if_provided,
};
pub use sdk_version::{get_sdk_version, SdkVersion};
pub use sink::{make_surreal2_sink, make_surreal3_sink};
